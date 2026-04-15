/* =========================================================
 * bptree.c — B+ Tree 구현
 *
 * 담당자 : 김용 (역할 A)
 * 금지   : 다른 팀원은 이 파일을 수정하지 않는다.
 *
 * 이 파일의 목표:
 *   - 단일 키 B+ Tree 구현
 *   - 같은 구현체를 id 트리와 age 트리에 공통으로 사용
 *   - age 트리에서 같은 key(같은 age)의 중복 저장 지원
 *   - range 결과를 항상 "key asc, same-key offsets asc" 로 고정
 *   - 성능 하네스가 최근 탐색의 노드 방문 수를 확인할 수 있게 지원
 *
 * ---------------------------------------------------------
 * 이 파일을 읽는 순서 추천
 * ---------------------------------------------------------
 * 1. 맨 위 설명으로 "트리 모양"과 "전체 흐름"을 먼저 잡는다.
 * 2. BPNode / BPValueList 구조를 보고
 *    "리프는 key + offset 목록", "내부 노드는 key + child" 라는
 *    큰 틀을 이해한다.
 * 3. bptree_insert() 와 bpnode_insert() 를 보면
 *    "리프 삽입 → split 발생 시 부모 전파 → 루트 승격" 흐름이 보인다.
 * 4. find_leaf(), bptree_search(), bptree_range_alloc() 을 보면
 *    "어떻게 원하는 리프를 찾고, 리프를 어떻게 순회하는지"가 보인다.
 * 5. 마지막으로 rollback_* 헬퍼를 보면
 *    "메모리 할당 실패 시 왜 트리를 원래 상태로 되돌리려 하는지"를 이해할 수 있다.
 *
 * ---------------------------------------------------------
 * 핵심 실행 흐름 요약
 * ---------------------------------------------------------
 * INSERT
 *   bptree_insert()
 *     → bpnode_insert(root)
 *     → 내부 노드면 맞는 child 로 내려감
 *     → 리프면 key 삽입 또는 duplicate bucket 추가
 *     → overflow 시 split 결과(did_split, promoted_key, right)를 반환
 *     → 부모는 그 split 결과를 받아 자기 key/child 배열에 반영
 *     → 루트도 split 되면 최상위에서 새 루트를 세워 height 증가
 *
 * SEARCH
 *   bptree_search()
 *     → find_leaf(key)
 *     → leaf_lower_bound()
 *     → key 가 있으면 대표 offset 반환, 없으면 -1
 *
 * RANGE
 *   bptree_range_alloc()
 *     → find_leaf(from)
 *     → 해당 리프의 시작 위치부터 검사
 *     → leaf->next 를 따라 오른쪽으로 이동
 *     → 범위 안의 key 들에 대해 offset bucket 을 차례대로 펼쳐 저장
 *   bptree_range()
 *     → 위의 전체 결과를 받아 호출자 버퍼 크기만큼만 복사
 *
 * FAILURE ROLLBACK
 *   split 직후 부모/루트용 메모리 할당이 실패하면
 *     → rollback_child_split() 으로 갈라진 노드를 다시 합치고
 *     → rollback_insert() 로 방금 넣은 key/value 자체도 제거해서
 *       "실패했지만 트리는 이미 바뀌어 있는" 상태를 막는다.
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/bptree.h"

#define IO_PAGE_SIZE 4096
#define IO_SIM_PAGES 256

/* =========================================================
 * 내부 자료구조 설명
 *
 * B+ Tree 노드는 두 종류다.
 *   1. 내부 노드: key 배열 + child 포인터 배열
 *   2. 리프 노드: key 배열 + offset 목록 배열 + next/prev 링크
 *
 * 중요한 구현 포인트:
 *   - id 트리는 보통 key 가 유일하지만, age 트리는 중복 key 가 가능하다.
 *   - 같은 key 가 여러 번 들어오면 "리프 엔트리를 여러 개 복제"하지 않고,
 *     하나의 key 엔트리에 offset 목록을 매단다.
 *   - 이렇게 하면 트리의 정렬 기준은 여전히 key 하나로 단순하게 유지되고,
 *     range 조회에서만 offset 목록을 펼쳐서 반환하면 된다.
 *
 * 결과적으로 range 반환 순서는 아래처럼 고정된다.
 *   1. 리프를 왼쪽에서 오른쪽으로 순회 → key 오름차순
 *   2. 같은 key 의 offset 목록을 오름차순으로 저장 → same-key offsets asc
 * ========================================================= */

/* 같은 key 에 매달린 offset 들을 담는 작은 버킷이다.
 * id 트리는 보통 1개만 담기지만, age 트리는 여러 개가 될 수 있다. */
typedef struct BPValueList {
    long *offsets;
    int   count;
    int   capacity;
} BPValueList;

/* BPNode 는 "내부 노드"와 "리프 노드"를 한 구조체로 표현한다.
 * 어느 필드를 실제로 쓰는지는 is_leaf 값으로 구분한다. */
typedef struct BPNode {
    int is_leaf;
    int key_count;

    int *keys;

    /* leaf 전용: 각 key 에 매달린 offset 목록 */
    BPValueList **values;

    /* internal 전용: 자식 포인터 */
    struct BPNode **children;

    /* leaf 간 순차 이동용 링크 */
    struct BPNode *next;
    struct BPNode *prev;
} BPNode;

/* 자식이 split 되었을 때 부모에게 돌려주는 요약 정보다.
 * 부모는 이 구조체만 보고
 *   - split 이 있었는지
 *   - 어느 key 를 separator 로 올려야 하는지
 *   - 새 오른쪽 노드는 무엇인지
 * 를 알 수 있다. */
typedef struct {
    int     did_split;
    int     promoted_key;
    BPNode *right;
} BPSplitResult;

/* range 결과를 미리 상한 없이 모으기 위한 동적 버퍼다.
 * bptree_range_alloc() 은 이 버퍼를 내부적으로 키워 가며 사용한다. */
typedef struct {
    long *offsets;
    int   count;
    int   capacity;
} BPRangeBuffer;

struct BPTree {
    int     order;          /* 노드당 최대 자식 수 */
    int     height;         /* 루트가 리프이면 1 */
    int     last_io_count;  /* 마지막 search/range 의 노드 방문 수 */
    BPNode *root;
};

/* =========================================================
 * I/O 시뮬레이션 헬퍼
 *
 * BPTREE_SIMULATE_IO=1 일 때는 tmpfile() 로 만든 임시 파일에서
 * 고정 크기 페이지를 읽으며 "노드 1개 방문" 비용을 흉내 낸다.
 *
 * 여기서 중요한 점:
 *   - 실제 디스크 엔진을 구현하는 것은 아니다.
 *   - 성능 하네스가 "트리 높이가 높을수록 더 많은 노드를 방문한다"는
 *     차이를 비교하기 쉽게 만들려는 보조 장치다.
 * ========================================================= */
#if BPTREE_SIMULATE_IO
static FILE *g_sim_file = NULL;
static long  g_sim_page_cursor = 0;
static int   g_sim_ready = 0;

static int sim_ensure_file(void) {
    if (g_sim_ready) return g_sim_file != NULL;

    g_sim_ready = 1;
    g_sim_file = tmpfile();
    if (!g_sim_file) return 0;

    unsigned char page[IO_PAGE_SIZE];
    memset(page, 0xA5, sizeof(page));

    for (int i = 0; i < IO_SIM_PAGES; i++) {
        if (fwrite(page, 1, sizeof(page), g_sim_file) != sizeof(page)) {
            fclose(g_sim_file);
            g_sim_file = NULL;
            return 0;
        }
    }

    fflush(g_sim_file);
    return 1;
}
#endif

static void sim_page_read(void) {
#if BPTREE_SIMULATE_IO
    unsigned char page[IO_PAGE_SIZE];

    if (!sim_ensure_file()) return;

    if (fseek(g_sim_file, g_sim_page_cursor * IO_PAGE_SIZE, SEEK_SET) != 0)
        rewind(g_sim_file);

    (void)fread(page, 1, sizeof(page), g_sim_file);
    g_sim_page_cursor = (g_sim_page_cursor + 1) % IO_SIM_PAGES;
#endif
}

/* 탐색 중 노드 하나를 방문할 때마다 호출된다.
 * 성능 하네스는 이 카운터를 통해 "이번 탐색이 몇 레벨을 탔는지"를 본다. */
static void record_node_visit(BPTree *tree) {
    if (!tree) return;
    tree->last_io_count++;
    sim_page_read();
}

/* ── value list 헬퍼 ──────────────────────────────────────
 *
 * 이 섹션은 "같은 key 아래 여러 offset 을 어떻게 관리하는가"를 담당한다.
 * age 중복 처리를 이해하려면 여기부터 보는 게 좋다.
 */

static BPValueList *valuelist_create(long offset) {
    BPValueList *list = (BPValueList *)calloc(1, sizeof(BPValueList));
    if (!list) return NULL;

    list->capacity = 4;
    list->offsets = (long *)calloc((size_t)list->capacity, sizeof(long));
    if (!list->offsets) {
        free(list);
        return NULL;
    }

    list->offsets[0] = offset;
    list->count = 1;
    return list;
}

static void valuelist_destroy(BPValueList *list) {
    if (!list) return;
    free(list->offsets);
    free(list);
}

static int valuelist_insert_sorted(BPValueList *list, long offset) {
    int insert_at = 0;

    if (!list) return -1;

    /* offset 도 오름차순으로 유지해야
     * range 결과를 "same-key offsets asc" 로 바로 펼칠 수 있다. */
    while (insert_at < list->count && list->offsets[insert_at] <= offset)
        insert_at++;

    if (list->count == list->capacity) {
        int new_capacity = list->capacity * 2;
        long *grown = (long *)realloc(list->offsets,
                                      (size_t)new_capacity * sizeof(long));
        if (!grown) return -1;
        list->offsets = grown;
        list->capacity = new_capacity;
    }

    for (int i = list->count; i > insert_at; i--)
        list->offsets[i] = list->offsets[i - 1];

    list->offsets[insert_at] = offset;
    list->count++;
    return 0;
}

static int valuelist_remove_one(BPValueList *list, long offset) {
    int remove_at = -1;

    if (!list) return -1;

    for (int i = 0; i < list->count; i++) {
        if (list->offsets[i] == offset) {
            remove_at = i;
            break;
        }
    }

    if (remove_at < 0) return -1;

    for (int i = remove_at; i + 1 < list->count; i++)
        list->offsets[i] = list->offsets[i + 1];

    list->count--;
    return 0;
}

/* ── node 헬퍼 ─────────────────────────────────────────────
 *
 * 이 섹션은 노드 생성/해제를 담당한다.
 * 삽입/탐색 로직을 보기 전에 "노드가 overflow 시 잠깐 얼마나 커질 수 있는지"를
 * 여기서 먼저 이해하면 split 코드가 더 잘 읽힌다.
 */

static BPNode *bpnode_create(int order, int is_leaf) {
    BPNode *node = (BPNode *)calloc(1, sizeof(BPNode));
    if (!node) return NULL;

    node->is_leaf = is_leaf;
    node->keys = (int *)calloc((size_t)order, sizeof(int));
    if (!node->keys) {
        free(node);
        return NULL;
    }

    if (is_leaf) {
        /* 리프는 overflow 시 order 개 key 까지 잠깐 담을 수 있어야 한다. */
        node->values = (BPValueList **)calloc((size_t)order,
                                              sizeof(BPValueList *));
        if (!node->values) {
            free(node->keys);
            free(node);
            return NULL;
        }
    } else {
        /* 내부 노드는 overflow 시 order+1 개 child 를 잠깐 담을 수 있어야 한다. */
        node->children = (BPNode **)calloc((size_t)(order + 1),
                                           sizeof(BPNode *));
        if (!node->children) {
            free(node->keys);
            free(node);
            return NULL;
        }
    }

    return node;
}

static void bpnode_destroy(BPNode *node) {
    if (!node) return;

    if (node->is_leaf) {
        for (int i = 0; i < node->key_count; i++)
            valuelist_destroy(node->values[i]);
        free(node->values);
    } else {
        for (int i = 0; i <= node->key_count; i++)
            bpnode_destroy(node->children[i]);
        free(node->children);
    }

    free(node->keys);
    free(node);
}

static int max_keys(const BPTree *tree) {
    return tree->order - 1;
}

/* 내부 노드에서 내려갈 자식을 고른다.
 * keys[i] 는 "오른쪽 자식의 첫 key" 역할을 한다.
 * 그래서 key 가 separator 와 같으면 오른쪽 자식으로 내려간다. */
static int internal_child_index(const BPNode *node, int key) {
    int idx = 0;
    while (idx < node->key_count && key >= node->keys[idx])
        idx++;
    return idx;
}

/* 리프에서 key 가 들어갈 첫 위치(lower bound)를 찾는다.
 * 같은 key 가 이미 있으면 그 key 위치를 반환한다. */
static int leaf_lower_bound(const BPNode *leaf, int key) {
    int lo = 0;
    int hi = leaf->key_count;

    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (leaf->keys[mid] < key)
            lo = mid + 1;
        else
            hi = mid;
    }

    return lo;
}

/* ── split 헬퍼 ────────────────────────────────────────────
 *
 * split 은 "가득 찬 노드를 반으로 나누고, 분리 기준 key 를 부모에게 올리는 과정"이다.
 * 아래 helper 들은 실제 배열 재배치만 담당한다.
 *
 * 예시 시나리오:
 *   order=4 인 리프에 key 가 [10, 20, 30, 40] 까지 들어가 overflow 가 났다고 하자.
 *   split_leaf_into_right() 는 이를
 *     left  = [10, 20]
 *     right = [30, 40]
 *   로 나누고, 부모에게는 "30 을 separator 로 사용하라"는 정보를 돌려준다.
 */

static void split_leaf_into_right(BPNode *leaf,
                                  BPNode *right,
                                  BPSplitResult *result) {
    int old_count = leaf->key_count;
    int split_at = old_count / 2;

    /* split 용 right 노드는 삽입 전에 미리 할당해 둔다.
     * 그래서 여기서는 메모리 할당 없이 구조만 재배치하고,
     * 실패가 나더라도 "에러 반환 전에 트리가 일부만 바뀌는" 상황을 막는다. */
    for (int i = split_at; i < old_count; i++) {
        int right_idx = right->key_count;
        right->keys[right_idx] = leaf->keys[i];
        right->values[right_idx] = leaf->values[i];
        right->key_count++;
        leaf->values[i] = NULL;
    }

    leaf->key_count = split_at;

    /* 리프 split 뒤에도 next/prev 연결이 유지되어야
     * range 조회가 "왼쪽에서 오른쪽으로 계속 읽기" 형태로 유지된다. */
    right->next = leaf->next;
    if (right->next) right->next->prev = right;
    right->prev = leaf;
    leaf->next = right;

    result->did_split = 1;
    result->promoted_key = right->keys[0];
    result->right = right;
}

static void split_internal_into_right(BPNode *node,
                                      BPNode *right,
                                      BPSplitResult *result) {
    int old_count = node->key_count;
    int split_at = old_count / 2;
    int promoted_key = node->keys[split_at];
    int right_key_idx = 0;
    int right_child_idx = 0;

    /* 내부 노드는 가운데 key 하나를 부모로 올리고,
     * 그 오른쪽 key/child 들을 새 노드로 옮긴다. */
    for (int i = split_at + 1; i < old_count; i++)
        right->keys[right_key_idx++] = node->keys[i];
    right->key_count = right_key_idx;

    for (int i = split_at + 1; i <= old_count; i++) {
        right->children[right_child_idx++] = node->children[i];
        node->children[i] = NULL;
    }

    node->key_count = split_at;

    result->did_split = 1;
    result->promoted_key = promoted_key;
    result->right = right;
}

/* ── rollback 헬퍼 ─────────────────────────────────────────
 *
 * split 후 부모 또는 루트 단계에서 추가 메모리 확보에 실패하면,
 * 이미 갈라진 노드와 방금 삽입한 key/value 를 되돌려야 한다.
 * 이 helper 들은 그 "실패 후 원상복구"만 담당한다.
 */

static void rollback_leaf_split(BPNode *left, BPNode *right) {
    int base = left->key_count;

    for (int i = 0; i < right->key_count; i++) {
        left->keys[base + i] = right->keys[i];
        left->values[base + i] = right->values[i];
        right->values[i] = NULL;
    }

    left->key_count += right->key_count;
    left->next = right->next;
    if (left->next) left->next->prev = left;

    bpnode_destroy(right);
}

static void rollback_internal_split(BPNode *left,
                                    int promoted_key,
                                    BPNode *right) {
    int base_keys = left->key_count;
    int base_children = left->key_count + 1;

    left->keys[base_keys] = promoted_key;
    for (int i = 0; i < right->key_count; i++)
        left->keys[base_keys + 1 + i] = right->keys[i];

    for (int i = 0; i <= right->key_count; i++) {
        left->children[base_children + i] = right->children[i];
        right->children[i] = NULL;
    }

    left->key_count = base_keys + 1 + right->key_count;
    bpnode_destroy(right);
}

static void rollback_child_split(BPNode *left_child,
                                 BPSplitResult *child_result) {
    if (!left_child || !child_result || !child_result->did_split ||
        !child_result->right)
        return;

    if (left_child->is_leaf)
        rollback_leaf_split(left_child, child_result->right);
    else
        rollback_internal_split(left_child,
                                child_result->promoted_key,
                                child_result->right);

    child_result->did_split = 0;
    child_result->right = NULL;
}

static int rollback_insert(BPNode *node, int key, long offset) {
    if (!node) return -1;

    if (node->is_leaf) {
        int pos = leaf_lower_bound(node, key);
        BPValueList *list = NULL;

        if (pos >= node->key_count || node->keys[pos] != key)
            return -1;

        list = node->values[pos];
        if (valuelist_remove_one(list, offset) != 0)
            return -1;

        /* 같은 key 아래 아직 다른 offset 이 남아 있으면
         * key 엔트리 자체는 유지한다. */
        if (list->count > 0)
            return 0;

        valuelist_destroy(list);
        for (int i = pos; i + 1 < node->key_count; i++) {
            node->keys[i] = node->keys[i + 1];
            node->values[i] = node->values[i + 1];
        }

        node->values[node->key_count - 1] = NULL;
        node->key_count--;
        return 0;
    }

    return rollback_insert(node->children[internal_child_index(node, key)],
                           key, offset);
}

/* ── 재귀 삽입 ─────────────────────────────────────────────
 *
 * bpnode_insert() 가 이 파일의 핵심이다.
 * 흐름은 크게 두 갈래다.
 *
 *   A. 현재 노드가 leaf 인 경우
 *      - duplicate key 면 bucket 에 offset 추가
 *      - 새 key 면 leaf 배열에 삽입
 *      - overflow 면 leaf split 수행
 *
 *   B. 현재 노드가 internal 인 경우
 *      - 맞는 child 하나를 골라 재귀적으로 내려감
 *      - child split 이 없으면 그대로 종료
 *      - child split 이 있으면 부모 배열에 separator/right child 반영
 *      - 부모도 overflow 면 internal split 수행
 *
 * 즉, split 은 "아래에서 발생하고 위로 전파"된다.
 */

static int bpnode_insert(BPTree *tree, BPNode *node,
                         int key, long offset, BPSplitResult *result) {
    result->did_split = 0;
    result->right = NULL;

    if (node->is_leaf) {
        int pos = leaf_lower_bound(node, key);
        BPNode *right = NULL;

        /* 같은 key 가 이미 있으면 새 엔트리를 만들지 않고
         * offset 목록에만 추가한다. age 중복 처리의 핵심이다. */
        if (pos < node->key_count && node->keys[pos] == key)
            return valuelist_insert_sorted(node->values[pos], offset);

        BPValueList *list = valuelist_create(offset);
        if (!list) return -1;

        /* 리프가 이미 가득 찼다면 split 에 필요한 새 리프를 먼저 확보한다.
         * 이 순서를 지켜야 할당 실패 시 "삽입은 실패했는데 트리는 바뀐 상태"가 되지 않는다. */
        if (node->key_count == max_keys(tree)) {
            right = bpnode_create(tree->order, 1);
            if (!right) {
                valuelist_destroy(list);
                return -1;
            }
        }

        for (int i = node->key_count; i > pos; i--) {
            node->keys[i] = node->keys[i - 1];
            node->values[i] = node->values[i - 1];
        }

        node->keys[pos] = key;
        node->values[pos] = list;
        node->key_count++;

        if (!right)
            return 0;

        split_leaf_into_right(node, right, result);
        return 0;
    }

    int child_idx = internal_child_index(node, key);
    BPNode *right = NULL;
    BPSplitResult child_result = {0, 0, NULL};

    /* 내부 노드는 직접 데이터를 저장하지 않는다.
     * 그래서 실제 삽입은 항상 child 쪽에서 먼저 일어난다. */
    if (bpnode_insert(tree, node->children[child_idx],
                      key, offset, &child_result) != 0)
        return -1;

    if (!child_result.did_split)
        return 0;

    /* 부모가 가득 차 있어도 실제로 자식 split 이 일어난 경우에만
     * 부모 split 용 새 노드를 확보한다. */
    if (node->key_count == max_keys(tree)) {
        right = bpnode_create(tree->order, 0);
        if (!right) {
            rollback_child_split(node->children[child_idx], &child_result);
            rollback_insert(node->children[child_idx], key, offset);
            return -1;
        }
    }

    for (int i = node->key_count; i > child_idx; i--)
        node->keys[i] = node->keys[i - 1];

    for (int i = node->key_count + 1; i > child_idx + 1; i--)
        node->children[i] = node->children[i - 1];

    node->keys[child_idx] = child_result.promoted_key;
    node->children[child_idx + 1] = child_result.right;
    node->key_count++;

    if (!right)
        return 0;

    split_internal_into_right(node, right, result);
    return 0;
}

/* range 전체 결과를 담는 동적 버퍼의 push 헬퍼다.
 * 큰 범위 쿼리도 상한 없이 받아낼 수 있게 버퍼를 자동 확장한다. */
static int range_buffer_push(BPRangeBuffer *buffer, long offset) {
    if (!buffer) return -1;

    if (buffer->count == buffer->capacity) {
        int new_capacity = buffer->capacity == 0 ? 16 : buffer->capacity * 2;
        long *grown = (long *)realloc(buffer->offsets,
                                      (size_t)new_capacity * sizeof(long));
        if (!grown) return -1;
        buffer->offsets = grown;
        buffer->capacity = new_capacity;
    }

    buffer->offsets[buffer->count++] = offset;
    return 0;
}

/* ── 탐색 헬퍼 ─────────────────────────────────────────────
 *
 * 탐색 계열 함수들은 먼저 "원하는 key 가 들어 있을 리프"를 찾는다.
 * 그 다음 리프 안에서만 key 를 확인한다.
 * B+ Tree 에서 실제 데이터는 리프에만 있으므로 이 흐름이 기본이다.
 *
 * last_io_count 는 "탐색 과정에서 몇 개의 노드를 방문했는가"를 나타낸다.
 * 그래서 내부 노드뿐 아니라 마지막 리프 방문도 카운트한다.
 */
static BPNode *find_leaf(BPTree *tree, int key) {
    BPNode *node = tree ? tree->root : NULL;

    while (node) {
        record_node_visit(tree);
        if (node->is_leaf)
            return node;
        node = node->children[internal_child_index(node, key)];
    }

    return NULL;
}

/* ── 디버그 출력 헬퍼 ────────────────────────────────────── */

static void print_indent(int depth) {
    for (int i = 0; i < depth; i++)
        printf("  ");
}

static void print_node(const BPNode *node, int depth) {
    if (!node) return;

    print_indent(depth);
    if (node->is_leaf) {
        printf("[leaf] ");
        for (int i = 0; i < node->key_count; i++) {
            printf("%d(%d)", node->keys[i], node->values[i]->count);
            if (i + 1 < node->key_count) printf(" | ");
        }
        printf("\n");
        return;
    }

    printf("[internal] ");
    for (int i = 0; i < node->key_count; i++) {
        printf("%d", node->keys[i]);
        if (i + 1 < node->key_count) printf(" | ");
    }
    printf("\n");

    for (int i = 0; i <= node->key_count; i++)
        print_node(node->children[i], depth + 1);
}

/* ── 생성 / 소멸 ─────────────────────────────────────────── */

BPTree *bptree_create(int order) {
    if (order < 3) order = 3;

    BPTree *tree = (BPTree *)calloc(1, sizeof(BPTree));
    if (!tree) return NULL;

    tree->order = order;
    tree->root = bpnode_create(order, 1);
    if (!tree->root) {
        free(tree);
        return NULL;
    }

    /* 루트가 곧 리프인 초기 상태이므로 높이는 1이다. */
    tree->height = 1;
    tree->last_io_count = 0;
    return tree;
}

void bptree_destroy(BPTree *tree) {
    if (!tree) return;
    bpnode_destroy(tree->root);
    free(tree);
}

/* ── 삽입 ────────────────────────────────────────────────── */

int bptree_insert(BPTree *tree, int key, long value) {
    BPSplitResult result = {0, 0, NULL};

    if (!tree || !tree->root) return -1;

    if (bpnode_insert(tree, tree->root, key, value, &result) != 0)
        return -1;

    if (result.did_split) {
        BPNode *new_root = bpnode_create(tree->order, 0);
        if (!new_root) {
            rollback_child_split(tree->root, &result);
            rollback_insert(tree->root, key, value);
            return -1;
        }

        new_root->keys[0] = result.promoted_key;
        new_root->children[0] = tree->root;
        new_root->children[1] = result.right;
        new_root->key_count = 1;

        tree->root = new_root;
        tree->height++;
    }

    return 0;
}

/* ── 탐색 ────────────────────────────────────────────────── */

long bptree_search(BPTree *tree, int key) {
    BPNode *leaf = NULL;
    int pos = 0;

    if (!tree || !tree->root) return -1;

    tree->last_io_count = 0;
    leaf = find_leaf(tree, key);
    if (!leaf) return -1;

    pos = leaf_lower_bound(leaf, key);
    if (pos >= leaf->key_count || leaf->keys[pos] != key)
        return -1;

    /* point search 는 주로 id 트리에서 사용된다.
     * 중복 key 가 있더라도 가장 작은 offset 을 대표값으로 돌려준다. */
    return leaf->values[pos]->offsets[0];
}

/* ── 범위 탐색 ─────────────────────────────────────────────
 *
 * bptree_range_alloc():
 *   범위 결과 전체를 동적 배열에 모아 반환한다.
 *   큰 범위도 잘리지 않게 전부 모을 수 있다는 점이 핵심이다.
 *
 * 예시 시나리오:
 *   age 리프들이 왼쪽부터
 *     [20]
 *     [25]   (이 key 아래 offset bucket 이 여러 개 달려 있음)
 *     [30, 35]
 *   라고 하자.
 *   bptree_range_alloc(tree, 25, 30, ...) 는
 *     - 먼저 25 가 들어 있는 리프를 찾고
 *     - 그 리프의 25 key bucket 을 모두 펼친 뒤
 *     - next 로 넘어가 30 까지 읽고
 *     - 35 를 보는 순간 멈춘다.
 *
 * bptree_range():
 *   위 전체 결과를 받아 호출자가 준 out[] 크기까지만 복사한다.
 *   즉 "전체 결과 생성"과 "호출자 버퍼에 잘라 담기"를 분리한 구조다.
 */

long *bptree_range_alloc(BPTree *tree, int from, int to, int *out_count) {
    BPNode *leaf = NULL;
    int pos = 0;
    int first_leaf = 1;
    BPRangeBuffer buffer = {0, 0, 0};

    if (out_count) *out_count = 0;

    if (!tree || !tree->root || !out_count) return NULL;
    if (from > to) return NULL;

    tree->last_io_count = 0;
    leaf = find_leaf(tree, from);
    if (!leaf) return NULL;

    pos = leaf_lower_bound(leaf, from);

    while (leaf) {
        /* find_leaf() 가 첫 리프 방문은 이미 카운트했다.
         * 이후 오른쪽 리프로 건너갈 때마다 추가 방문을 기록한다. */
        if (!first_leaf)
            record_node_visit(tree);

        while (pos < leaf->key_count) {
            int key = leaf->keys[pos];
            BPValueList *list = leaf->values[pos];

            if (key > to) {
                *out_count = buffer.count;
                if (buffer.count == 0) {
                    free(buffer.offsets);
                    return NULL;
                }
                return buffer.offsets;
            }

            if (key >= from) {
                for (int i = 0; i < list->count; i++) {
                    if (range_buffer_push(&buffer, list->offsets[i]) != 0) {
                        free(buffer.offsets);
                        *out_count = 0;
                        return NULL;
                    }
                }
            }
            pos++;
        }

        leaf = leaf->next;
        pos = 0;
        first_leaf = 0;
    }

    *out_count = buffer.count;
    if (buffer.count == 0) {
        free(buffer.offsets);
        return NULL;
    }

    return buffer.offsets;
}

int bptree_range(BPTree *tree, int from, int to, long *out, int max_count) {
    int count = 0;
    int copied = 0;
    long *all_offsets = NULL;

    if (!tree || !tree->root || !out || max_count <= 0) return 0;
    if (from > to) return 0;

    all_offsets = bptree_range_alloc(tree, from, to, &count);
    if (!all_offsets || count <= 0) return 0;

    copied = (count < max_count) ? count : max_count;
    memcpy(out, all_offsets, (size_t)copied * sizeof(long));
    free(all_offsets);
    return copied;
}

/* ── 정보 조회 ───────────────────────────────────────────── */

int bptree_height(BPTree *tree) {
    if (!tree) return 0;
    return tree->height;
}

int bptree_last_io(BPTree *tree) {
    if (!tree) return 0;
    return tree->last_io_count;
}

void bptree_print(BPTree *tree) {
    if (!tree || !tree->root) {
        printf("[bptree] NULL\n");
        return;
    }

    printf("[bptree] order=%d height=%d\n", tree->order, tree->height);
    print_node(tree->root, 0);
}
