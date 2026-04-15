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
 *
 * 구현 체크리스트:
 *   [x] BPTree 내부 구조체 (BPNode) 정의
 *   [x] 단일 키: bptree_insert (노드 분열 포함)
 *   [x] 단일 키: bptree_search (BPTREE_SIMULATE_IO 지원)
 *   [x] 단일 키: bptree_range  (리프 링크드 리스트 순회)
 *   [x] 단일 키: 중복 key 지원 (age 트리 대응)
 *   [x] 단일 키: bptree_height
 *   [x] bptree_print (디버그 출력)
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
 * 4. find_leaf(), bptree_search(), bptree_range() 를 보면
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
 *   bptree_range()
 *     → find_leaf(from)
 *     → 해당 리프의 시작 위치부터 검사
 *     → leaf->next 를 따라 오른쪽으로 이동
 *     → 범위 안의 key 들에 대해 offset bucket 을 차례대로 펼쳐 저장
 *
 * FAILURE ROLLBACK
 *   split 직후 부모/루트용 메모리 할당이 실패하면
 *     → rollback_child_split() 으로 갈라진 노드를 다시 합치고
 *     → rollback_insert() 로 방금 넣은 key/value 자체도 제거해서
 *       "실패했지만 트리는 이미 바뀌어 있는" 상태를 막는다.
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include "../../include/bptree.h"

/* ── 플랫폼별 sleep ──────────────────────────────────────── */
#if BPTREE_SIMULATE_IO
#  ifdef _WIN32
#    include <windows.h>
#    define IO_SLEEP() Sleep(DISK_IO_DELAY_US / 1000 > 0 \
                              ? DISK_IO_DELAY_US / 1000 : 1)
#  else
#    include <unistd.h>
#    define IO_SLEEP() usleep(DISK_IO_DELAY_US)
#  endif
#else
#  define IO_SLEEP() ((void)0)
#endif

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

struct BPTree {
    int     order;   /* 노드당 최대 자식 수 */
    int     height;  /* 루트가 리프이면 1 */
    BPNode *root;
};

/* ── value list 헬퍼 ──────────────────────────────────────
 *
 * 이 섹션은 "같은 key 아래 여러 offset 을 어떻게 관리하는가"를 담당한다.
 * age 중복 처리를 이해하려면 여기부터 보는 게 좋다.
 */

static BPValueList *valuelist_create(long offset) {
    BPValueList *list = (BPValueList *)calloc(1, sizeof(BPValueList));
    if (!list) return NULL;

    list->capacity = 4;
    list->offsets  = (long *)calloc((size_t)list->capacity, sizeof(long));
    if (!list->offsets) {
        free(list);
        return NULL;
    }

    list->offsets[0] = offset;
    list->count      = 1;
    return list;
}

static void valuelist_destroy(BPValueList *list) {
    if (!list) return;
    free(list->offsets);
    free(list);
}

static int valuelist_insert_sorted(BPValueList *list, long offset) {
    int insert_at = 0;
    long *grown = NULL;

    if (!list) return -1;

    /* offset 도 오름차순으로 유지해야
     * range 결과를 "same-key offsets asc" 로 바로 펼칠 수 있다. */
    while (insert_at < list->count && list->offsets[insert_at] <= offset)
        insert_at++;

    if (list->count == list->capacity) {
        int new_capacity = list->capacity * 2;
        grown = (long *)realloc(list->offsets,
                                (size_t)new_capacity * sizeof(long));
        if (!grown) return -1;
        list->offsets  = grown;
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
    node->keys    = (int *)calloc((size_t)order, sizeof(int));
    if (!node->keys) {
        free(node);
        return NULL;
    }

    if (is_leaf) {
        /* 리프는 overflow 시 order 개 key 까지 잠시 담을 수 있어야 한다. */
        node->values = (BPValueList **)calloc((size_t)order,
                                              sizeof(BPValueList *));
        if (!node->values) {
            free(node->keys);
            free(node);
            return NULL;
        }
    } else {
        /* 내부 노드는 overflow 시 order+1 개 child 를 잠시 담을 수 있어야 한다. */
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
 * split 은 "가득 찬 노드를 반으로 나누고, 오른쪽 절반의 첫 key 를 부모에게 올리는 과정"이다.
 * 아래 helper 들은 실제로 배열 재배치를 수행하는 순수 split 작업만 담당한다.
 * 부모가 이 결과를 어떻게 받아들이는지는 bpnode_insert() 에서 이어진다.
 *
 * 예시 시나리오:
 *   order=4 인 리프에 key 가 [10, 20, 30, 40] 까지 들어가 overflow 가 났다고 가정하자.
 *   split_leaf_into_right() 는 이를 대략
 *     left  = [10, 20]
 *     right = [30, 40]
 *   형태로 나누고, 부모에게는 "30 을 separator 로 올리고 right 를 새 자식으로 연결하라"
 *   는 의미의 result 를 돌려준다.
 */

static void split_leaf_into_right(BPNode *leaf,
                                  BPNode *right,
                                  BPSplitResult *result) {
    int old_count = leaf->key_count;
    int split_at  = old_count / 2;

    /* split 용 right 노드는 삽입 전에 미리 할당해 둔다.
     * 그래서 여기서는 메모리 할당 없이 구조만 재배치하고,
     * 실패가 나더라도 "에러 반환 전에 트리가 일부만 바뀌는" 상황을 막는다. */
    for (int i = split_at; i < old_count; i++) {
        int right_idx = right->key_count;
        right->keys[right_idx]   = leaf->keys[i];
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
    leaf->next  = right;

    result->did_split    = 1;
    result->promoted_key = right->keys[0];
    result->right        = right;
}

static void split_internal_into_right(BPNode *node,
                                      BPNode *right,
                                      BPSplitResult *result) {
    int old_count      = node->key_count;
    int split_at       = old_count / 2;
    int promoted_key   = node->keys[split_at];
    int right_key_idx  = 0;
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

    result->did_split    = 1;
    result->promoted_key = promoted_key;
    result->right        = right;
}

/* ── rollback 헬퍼 ─────────────────────────────────────────
 *
 * split 후 부모 또는 루트 단계에서 추가 메모리 확보에 실패하면,
 * 이미 갈라진 노드와 방금 삽입한 key/value 를 되돌려야 한다.
 * 이 helper 들은 그 "실패 후 원상복구"만 담당한다.
 *
 * 예시 시나리오:
 *   1. leaf insert 는 성공해서 [10, 20, 30] 이 되었고 split 도 일어났다.
 *   2. 그런데 부모가 그 split 결과를 받아들이기 위한 새 노드 할당에 실패했다.
 *   3. 이때 rollback_child_split() 은 갈라진 left/right 를 다시 합친다.
 *   4. 이어서 rollback_insert() 가 방금 넣은 30 자체도 제거한다.
 *   5. 최종적으로 트리는 "아예 삽입을 시도하지 않았던 상태"로 돌아간다.
 */

static void rollback_leaf_split(BPNode *left, BPNode *right) {
    int base = left->key_count;

    /* split 을 취소할 때는 오른쪽으로 보냈던 엔트리들을
     * 왼쪽 노드 끝으로 다시 붙이면 된다. */
    for (int i = 0; i < right->key_count; i++) {
        left->keys[base + i]   = right->keys[i];
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
    if (!left_child || !child_result || !child_result->did_split || !child_result->right)
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
            node->keys[i]   = node->keys[i + 1];
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
 *
 * 예시 시나리오 1: leaf insert 만으로 끝나는 경우
 *   root 가 leaf 이고 key 가 [10, 20] 인 상태에서 15 를 넣으면
 *   같은 leaf 안에서 [10, 15, 20] 으로 끝난다. split 결과는 없다.
 *
 * 예시 시나리오 2: leaf split 이 부모로 올라가는 경우
 *   부모 internal 아래 leaf 가 [10, 20, 30, 40] 으로 overflow 되면
 *   leaf split 결과가 "30 + new right" 형태로 부모에게 올라간다.
 *   부모는 자기 key/child 배열에 이를 끼워 넣고, 자기 overflow 여부를 다시 판단한다.
 */

static int bpnode_insert(BPTree *tree, BPNode *node,
                         int key, long offset, BPSplitResult *result) {
    result->did_split = 0;
    result->right     = NULL;

    if (node->is_leaf) {
        int pos = leaf_lower_bound(node, key);
        BPNode *right = NULL;

        /* 같은 key 가 이미 있으면 새 엔트리를 만들지 않고
         * offset 목록에만 추가한다. age 중복 처리의 핵심이다. */
        if (pos < node->key_count && node->keys[pos] == key)
            return valuelist_insert_sorted(node->values[pos], offset);

        BPValueList *list = valuelist_create(offset);
        if (!list) return -1;

        /* 리프가 이미 가득 찼다면, split 에 필요한 새 리프를 먼저 확보한다.
         * 이 순서를 지켜야 할당 실패 시 "삽입은 실패했는데 트리는 바뀐 상태"가 되지 않는다. */
        if (node->key_count == max_keys(tree)) {
            right = bpnode_create(tree->order, 1);
            if (!right) {
                valuelist_destroy(list);
                return -1;
            }
        }

        /* 새 key 를 넣기 위해 오른쪽 엔트리들을 한 칸씩 민다.
         * 정렬 배열을 유지하려면 항상 삽입 위치 뒤쪽만 이동시키면 된다. */
        for (int i = node->key_count; i > pos; i--) {
            node->keys[i]   = node->keys[i - 1];
            node->values[i] = node->values[i - 1];
        }

        node->keys[pos]   = key;
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

    /* 부모가 가득 차 있어도, 실제로 자식 split 이 일어난 경우에만
     * 부모 split 용 새 노드를 확보한다. 이렇게 해야
     * "조상은 full 이지만 이번 삽입은 split 이 필요 없는" 경우까지
     * 불필요하게 실패시키지 않는다.
     *
     * 반대로 이 시점의 할당이 실패하면, 방금 일어난 자식 split 을
     * 다시 합쳐서 삽입 전 상태로 되돌린다. */
    if (node->key_count == max_keys(tree)) {
        right = bpnode_create(tree->order, 0);
        if (!right) {
            rollback_child_split(node->children[child_idx], &child_result);
            rollback_insert(node->children[child_idx], key, offset);
            return -1;
        }
    }

    /* 자식이 split 되었으면, 부모 내부 노드에 separator key 와
     * 새 오른쪽 자식 포인터를 끼워 넣는다. */
    for (int i = node->key_count; i > child_idx; i--)
        node->keys[i] = node->keys[i - 1];

    for (int i = node->key_count + 1; i > child_idx + 1; i--)
        node->children[i] = node->children[i - 1];

    node->keys[child_idx]         = child_result.promoted_key;
    node->children[child_idx + 1] = child_result.right;
    node->key_count++;

    if (!right)
        return 0;

    split_internal_into_right(node, right, result);
    return 0;
}

/* ── 탐색 헬퍼 ─────────────────────────────────────────────
 *
 * 탐색 계열 함수들은 먼저 "원하는 key 가 들어 있을 리프"를 찾는다.
 * 그 다음 리프 안에서만 key 를 확인한다.
 * B+ Tree 에서 실제 데이터는 리프에만 있으므로 이 흐름이 기본이다.
 */

static BPNode *find_leaf(BPTree *tree, int key) {
    BPNode *node = tree ? tree->root : NULL;

    while (node && !node->is_leaf) {
        /* 높이 차이를 시간으로 보여주기 위해, 내부 레벨을 내려갈 때마다
         * I/O 지연을 흉내 낸다. */
        IO_SLEEP();
        node = node->children[internal_child_index(node, key)];
    }

    return node;
}

/* ── 디버그 출력 헬퍼 ──────────────────────────────────────
 *
 * 구조를 직접 눈으로 확인할 때 쓰는 영역이다.
 * leaf 출력에서 `key(count)` 형태를 쓰는 이유는
 * 같은 key 아래 offset bucket 크기도 같이 보이게 하려는 것이다.
 */

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

/* ── 생성 / 소멸 ───────────────────────────────────────────
 *
 * 외부 API 쪽의 가장 바깥 레이어다.
 * 여기서는 트리 전체 생애주기만 다루고,
 * 실제 삽입/탐색의 세부 동작은 아래 섹션들에 위임한다.
 */

BPTree *bptree_create(int order) {
    if (order < 3) order = 3;

    BPTree *tree = (BPTree *)calloc(1, sizeof(BPTree));
    if (!tree) return NULL;

    tree->order = order;
    tree->root  = bpnode_create(order, 1);
    if (!tree->root) {
        free(tree);
        return NULL;
    }

    /* 루트가 곧 리프인 초기 상태이므로 높이는 1이다. */
    tree->height = 1;
    return tree;
}

void bptree_destroy(BPTree *tree) {
    if (!tree) return;
    bpnode_destroy(tree->root);
    free(tree);
}

/* ── 삽입 ──────────────────────────────────────────────────
 *
 * top-level insert 흐름:
 *   1. root 기준으로 재귀 삽입 시작
 *   2. 아래쪽에서 split 이 올라오면 result.did_split 으로 받음
 *   3. root split 이 실제로 발생했을 때만 새 루트 생성
 *   4. 새 루트가 생기면 height 를 1 증가
 */

int bptree_insert(BPTree *tree, int key, long value) {
    BPSplitResult result = {0, 0, NULL};
    BPNode *new_root = NULL;

    if (!tree || !tree->root) return -1;

    if (bpnode_insert(tree, tree->root, key, value, &result) != 0)
        return -1;

    if (result.did_split) {
        /* 루트 split 이 실제로 일어난 경우에만 새 루트를 확보한다.
         * 만약 이 시점의 할당이 실패하면, 루트의 두 반쪽을 다시 합쳐서
         * 삽입 전 상태로 되돌린 뒤 실패를 반환한다. */
        new_root = bpnode_create(tree->order, 0);
        if (!new_root) {
            rollback_child_split(tree->root, &result);
            rollback_insert(tree->root, key, value);
            return -1;
        }

        new_root->keys[0]     = result.promoted_key;
        new_root->children[0] = tree->root;
        new_root->children[1] = result.right;
        new_root->key_count   = 1;

        tree->root = new_root;
        tree->height++;
    }

    return 0;
}

/* ── 탐색 ──────────────────────────────────────────────────
 *
 * point search 흐름은 단순하다.
 *   find_leaf(key) → lower_bound → key 일치 여부 확인 → 대표 offset 반환
 *
 * id 트리는 보통 이 함수로 바로 찾고,
 * age 트리는 duplicate 가능성이 크므로 range 쪽이 더 자주 쓰인다.
 *
 * 예시 시나리오:
 *   key=25 를 찾는다고 하면, find_leaf() 가 25 가 있어야 할 리프까지 내려간다.
 *   그 리프에서 leaf_lower_bound() 로 25 의 첫 위치를 찾고,
 *   실제 key 가 25 와 같으면 대표 offset 하나를 돌려준다.
 */

long bptree_search(BPTree *tree, int key) {
    BPNode *leaf = NULL;
    int pos = 0;

    if (!tree || !tree->root) return -1;

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
 * range 흐름:
 *   1. from 이 속할 첫 리프를 찾는다.
 *   2. 그 리프에서 from 이상의 첫 위치를 구한다.
 *   3. 현재 리프를 끝까지 읽는다.
 *   4. next 로 오른쪽 리프로 넘어가며 같은 작업을 반복한다.
 *   5. key > to 가 되는 순간 즉시 종료한다.
 *
 * 이 함수가 빠르게 동작하려면 리프 next 링크가 꼭 살아 있어야 한다.
 *
 * 예시 시나리오:
 *   age 리프들이 왼쪽부터
 *     [20]
 *     [25]   (이 key 아래 offset bucket 이 여러 개 달려 있음)
 *     [30, 35]
 *   라고 하자.
 *   bptree_range(tree, 25, 30, ...) 는
 *     - 먼저 25 가 들어 있는 리프를 찾고
 *     - 그 리프의 25 key 를 읽고, 그 key 의 offset bucket 을 모두 펼친 뒤
 *     - next 로 넘어가 30 까지 읽고
 *     - 35 를 보는 순간 멈춘다.
 *   그래서 결과는 25 들의 offset 들이 먼저, 그 다음 30 의 offset 들이 온다.
 */

int bptree_range(BPTree *tree, int from, int to,
                 long *out, int max_count) {
    BPNode *leaf = NULL;
    int pos = 0;
    int out_count = 0;

    if (!tree || !tree->root || !out || max_count <= 0) return 0;
    if (from > to) return 0;

    leaf = find_leaf(tree, from);
    if (!leaf) return 0;

    pos = leaf_lower_bound(leaf, from);

    while (leaf && out_count < max_count) {
        while (pos < leaf->key_count && out_count < max_count) {
            int key = leaf->keys[pos];
            BPValueList *list = leaf->values[pos];

            if (key > to)
                return out_count;

            if (key >= from) {
                /* 반환 순서를 고정하기 위해,
                 * 같은 key 의 offset 목록도 오름차순으로 저장/순회한다. */
                for (int i = 0; i < list->count && out_count < max_count; i++)
                    out[out_count++] = list->offsets[i];
            }
            pos++;
        }

        leaf = leaf->next;
        pos  = 0;
    }

    return out_count;
}

/* ── 정보 조회 ───────────────────────────────────────────── */

int bptree_height(BPTree *tree) {
    if (!tree) return 0;
    return tree->height;
}

void bptree_print(BPTree *tree) {
    if (!tree || !tree->root) {
        printf("[bptree] NULL\n");
        return;
    }

    printf("[bptree] order=%d height=%d\n", tree->order, tree->height);
    print_node(tree->root, 0);
}
