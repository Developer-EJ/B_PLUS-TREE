#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/bptree.h"

/*
 * bptree.c
 *
 * 이 구현은 두 인덱스가 공통으로 사용하는 B+Tree 본체다.
 * - id 트리:  키=id,  값=file offset
 * - age 트리: 키=age, 값=file offset
 *
 * age는 중복 키가 가능하므로, 리프 엔트리 하나가 여러 offset을
 * 들고 있을 수 있다.
 *
 * 추가로 sim 빌드에서는 노드 방문마다 "페이지 1번 읽기"에 해당하는
 * 시뮬레이션을 수행하고, 마지막 조회의 방문 횟수를 tree_io처럼 기록한다.
 */

#define IO_PAGE_SIZE 4096
#define IO_SIM_PAGES 256

/* 같은 키에 매달린 여러 offset 목록이다. age 인덱스에서 특히 중요하다. */
typedef struct BPValueList {
    long *offsets;
    int   count;
    int   capacity;
} BPValueList;

/*
 * B+Tree 노드 구조.
 * - 리프 노드면 values[] 사용
 * - 내부 노드면 children[] 사용
 * - 리프끼리는 next/prev로 이어서 범위 탐색을 빠르게 한다.
 */
typedef struct BPNode {
    int is_leaf;
    int key_count;

    int *keys;
    BPValueList **values;
    struct BPNode **children;

    struct BPNode *next;
    struct BPNode *prev;
} BPNode;

/* 삽입 재귀 도중 자식 분할 결과를 부모에게 전달할 때 쓰는 구조체다. */
typedef struct {
    int     did_split;
    int     promoted_key;
    BPNode *right;
} BPSplitResult;

/* range 결과 offset들을 동적으로 쌓아 두는 버퍼다. */
typedef struct {
    long *offsets;
    int   count;
    int   capacity;
} BPRangeBuffer;

struct BPTree {
    int     order;
    int     height;
    int     last_io_count;
    BPNode *root;
};

#if BPTREE_SIMULATE_IO
static FILE *g_sim_file = NULL;
static long  g_sim_page_cursor = 0;
static int   g_sim_ready = 0;

/*
 * sim 모드에서 재사용할 임시 파일을 한 번만 준비한다.
 * 매 노드 방문 때마다 open/close 하지 않고, 이미 열린 파일에서
 * 고정 크기 페이지를 read 하도록 만들기 위한 준비 단계다.
 */
static int sim_ensure_file(void) {
    if (g_sim_ready) return g_sim_file != NULL;

    g_sim_ready = 1;
    g_sim_file = tmpfile();
    if (!g_sim_file) return 0;

    unsigned char page[IO_PAGE_SIZE];
    memset(page, 0xA5, sizeof(page));

    /*
     * 같은 내용을 가진 페이지를 여러 개 미리 써 둔다.
     * 이후에는 이 파일 안에서 페이지 위치만 바꿔 가며 read 한다.
     */
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

/* sim 모드에서 페이지 하나를 읽었다고 가정하는 작은 읽기 동작이다. */
static void sim_page_read(void) {
#if BPTREE_SIMULATE_IO
    unsigned char page[IO_PAGE_SIZE];

    if (!sim_ensure_file()) return;

    /* 현재 가리키는 가상 페이지 위치로 이동해서 한 페이지를 읽는다. */
    if (fseek(g_sim_file, g_sim_page_cursor * IO_PAGE_SIZE, SEEK_SET) != 0)
        rewind(g_sim_file);

    (void)fread(page, 1, sizeof(page), g_sim_file);
    /* 다음 방문은 다른 페이지를 읽었다고 가정하기 위해 커서를 순환시킨다. */
    g_sim_page_cursor = (g_sim_page_cursor + 1) % IO_SIM_PAGES;
#endif
}

/* 노드 방문 1회를 기록하고, sim 모드면 페이지 read도 함께 수행한다. */
static void record_node_visit(BPTree *tree) {
    if (!tree) return;
    tree->last_io_count++;
    sim_page_read();
}

/* offset 하나로 시작하는 값 목록을 만든다. */
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

/* 값 목록 메모리를 정리한다. */
static void valuelist_destroy(BPValueList *list) {
    if (!list) return;
    free(list->offsets);
    free(list);
}

/* 같은 키 내부에서는 offset 오름차순이 유지되도록 삽입한다. */
static int valuelist_insert_sorted(BPValueList *list, long offset) {
    int insert_at = 0;

    if (!list) return -1;

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

    /* insert_at 뒤 원소들을 한 칸씩 밀어 자리를 만든다. */
    for (int i = list->count; i > insert_at; i--)
        list->offsets[i] = list->offsets[i - 1];

    list->offsets[insert_at] = offset;
    list->count++;
    return 0;
}

/* 되돌리기 처리 등에 쓰기 위해 값 목록에서 offset 하나를 제거한다. */
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

/* 리프 / 내부 노드 여부에 맞는 새 노드를 만든다. */
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
        node->values = (BPValueList **)calloc((size_t)order,
                                              sizeof(BPValueList *));
        if (!node->values) {
            free(node->keys);
            free(node);
            return NULL;
        }
    } else {
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

/* 노드 이하의 모든 메모리를 재귀적으로 해제한다. */
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

/* 한 노드에 들어갈 수 있는 최대 key 수는 order - 1 이다. */
static int max_keys(const BPTree *tree) {
    return tree->order - 1;
}

/* 내부 노드에서 키가 내려가야 할 자식 위치를 찾는다. */
static int internal_child_index(const BPNode *node, int key) {
    int idx = 0;
    while (idx < node->key_count && key >= node->keys[idx])
        idx++;
    return idx;
}

/* 리프에서 키가 들어갈 첫 위치를 찾는다. */
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

/* 리프가 넘쳤을 때 오른쪽 리프로 반을 나누고 연결도 유지한다. */
static void split_leaf_into_right(BPNode *leaf,
                                  BPNode *right,
                                  BPSplitResult *result) {
    int old_count = leaf->key_count;
    int split_at = old_count / 2;

    /* split 지점 오른쪽 절반을 새 leaf로 옮긴다. */
    for (int i = split_at; i < old_count; i++) {
        int right_idx = right->key_count;
        right->keys[right_idx] = leaf->keys[i];
        right->values[right_idx] = leaf->values[i];
        right->key_count++;
        leaf->values[i] = NULL;
    }

    leaf->key_count = split_at;

    /*
     * range scan이 leaf 연결 리스트를 따라가기 때문에
     * split 뒤에도 next/prev 링크를 반드시 유지해야 한다.
     */
    right->next = leaf->next;
    if (right->next) right->next->prev = right;
    right->prev = leaf;
    leaf->next = right;

    /* 부모에는 "오른쪽 leaf의 첫 key"가 separator로 올라간다. */
    result->did_split = 1;
    result->promoted_key = right->keys[0];
    result->right = right;
}

/* 내부 노드가 넘쳤을 때 가운데 키를 올리고 오른쪽 노드를 만든다. */
static void split_internal_into_right(BPNode *node,
                                      BPNode *right,
                                      BPSplitResult *result) {
    int old_count = node->key_count;
    int split_at = old_count / 2;
    int promoted_key = node->keys[split_at];
    int right_key_idx = 0;
    int right_child_idx = 0;

    /* 가운데 key는 부모로 승격되므로, 오른쪽 노드에는 split_at+1부터 복사한다. */
    for (int i = split_at + 1; i < old_count; i++)
        right->keys[right_key_idx++] = node->keys[i];
    right->key_count = right_key_idx;

    /* 오른쪽 노드는 승격된 key의 오른쪽 자식들만 넘겨받는다. */
    for (int i = split_at + 1; i <= old_count; i++) {
        right->children[right_child_idx++] = node->children[i];
        node->children[i] = NULL;
    }

    node->key_count = split_at;

    result->did_split = 1;
    result->promoted_key = promoted_key;
    result->right = right;
}

/* 리프 분할 도중 상위 단계에서 실패하면 원래 상태로 되돌린다. */
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

/* 내부 노드 분할 도중 상위 단계에서 실패하면 원래 상태로 되돌린다. */
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

/* 자식 노드 분할 결과를 종류에 맞춰 되돌린다. */
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

/* 삽입 중간 실패 시, 실제로 꽂아 넣은 키/offset도 재귀적으로 되돌린다. */
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

        /* 같은 key에 offset이 아직 남아 있으면 key 엔트리는 유지한다. */
        if (list->count > 0)
            return 0;

        /* 마지막 offset까지 빠졌다면 key 자체를 leaf에서 제거한다. */
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

/*
 * B+Tree 삽입의 재귀 본체다.
 * 리프면 직접 삽입하고, 내부 노드면 자식에 내려가 삽입한 뒤
 * 필요하면 자식 분할 결과를 현재 노드에 반영한다.
 */
static int bpnode_insert(BPTree *tree, BPNode *node,
                         int key, long offset, BPSplitResult *result) {
    result->did_split = 0;
    result->right = NULL;

    if (node->is_leaf) {
        int pos = leaf_lower_bound(node, key);
        BPNode *right = NULL;

        /* 같은 key가 이미 있으면 새 leaf entry를 만들지 않고 offset만 추가한다. */
        if (pos < node->key_count && node->keys[pos] == key)
            return valuelist_insert_sorted(node->values[pos], offset);

        BPValueList *list = valuelist_create(offset);
        if (!list) return -1;

        /*
         * 현재 leaf가 가득 찼다면 미리 오른쪽 노드를 준비해 둔다.
         * 실제 split은 삽입 후 한 번에 수행한다.
         */
        if (node->key_count == max_keys(tree)) {
            right = bpnode_create(tree->order, 1);
            if (!right) {
                valuelist_destroy(list);
                return -1;
            }
        }

        /* 삽입 위치 뒤의 key/value를 한 칸 밀어 새 key 자리를 만든다. */
        for (int i = node->key_count; i > pos; i--) {
            node->keys[i] = node->keys[i - 1];
            node->values[i] = node->values[i - 1];
        }

        node->keys[pos] = key;
        node->values[pos] = list;
        node->key_count++;

        if (!right)
            return 0;

        /* overflow였다면 여기서 leaf를 실제로 둘로 나눈다. */
        split_leaf_into_right(node, right, result);
        return 0;
    }

    int child_idx = internal_child_index(node, key);
    BPNode *right = NULL;
    BPSplitResult child_result = {0, 0, NULL};

    if (bpnode_insert(tree, node->children[child_idx],
                      key, offset, &child_result) != 0)
        return -1;

    /* 자식이 split되지 않았다면 부모는 아무것도 할 일이 없다. */
    if (!child_result.did_split)
        return 0;

    /* 자식 split 결과를 꽂기 전에, 부모 자신도 overflow인지 확인한다. */
    if (node->key_count == max_keys(tree)) {
        right = bpnode_create(tree->order, 0);
        if (!right) {
            rollback_child_split(node->children[child_idx], &child_result);
            rollback_insert(node->children[child_idx], key, offset);
            return -1;
        }
    }

    /* 승격된 separator key가 들어갈 자리를 만들기 위해 오른쪽으로 민다. */
    for (int i = node->key_count; i > child_idx; i--)
        node->keys[i] = node->keys[i - 1];

    /* children 배열은 key보다 하나 더 많으므로 +1 위치까지 함께 밀어야 한다. */
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

/* 범위 결과용 동적 버퍼에 offset 하나를 추가한다. */
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

/* 루트부터 내려가 키가 속한 리프를 찾는다. 방문할 때마다 tree_io도 기록한다. */
static BPNode *find_leaf(BPTree *tree, int key) {
    BPNode *node = tree ? tree->root : NULL;

    while (node) {
        record_node_visit(tree);
        if (node->is_leaf)
            return node;
        /* 현재 internal node의 separator를 기준으로 다음 child를 고른다. */
        node = node->children[internal_child_index(node, key)];
    }

    return NULL;
}

/* 출력용 들여쓰기 헬퍼다. */
static void print_indent(int depth) {
    for (int i = 0; i < depth; i++)
        printf("  ");
}

/* 디버깅용 트리 출력 함수의 재귀 본체다. */
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

/* 최소 order를 보정한 뒤, 비어 있는 leaf 하나를 가진 새 트리를 만든다. */
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

    tree->height = 1;
    tree->last_io_count = 0;
    return tree;
}

/* 트리 전체 메모리를 해제한다. */
void bptree_destroy(BPTree *tree) {
    if (!tree) return;
    bpnode_destroy(tree->root);
    free(tree);
}

/* 공개 insert 함수다. root split이 발생하면 새 root를 올린다. */
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

/* 단건 탐색: 키 하나를 찾아 해당 키의 첫 번째 offset을 돌려준다. */
long bptree_search(BPTree *tree, int key) {
    BPNode *leaf = NULL;
    int pos = 0;

    if (!tree || !tree->root) return -1;

    tree->last_io_count = 0;
    leaf = find_leaf(tree, key);
    if (!leaf) return -1;

    /* leaf 안에서는 lower_bound 결과가 실제 key와 같은지 다시 확인해야 한다. */
    pos = leaf_lower_bound(leaf, key);
    if (pos >= leaf->key_count || leaf->keys[pos] != key)
        return -1;

    /* point search는 같은 key의 첫 번째 offset 하나만 반환한다. */
    return leaf->values[pos]->offsets[0];
}

/*
 * 제한 없는 범위 탐색이다.
 * from 이상인 첫 리프를 찾은 뒤, 리프 연결 리스트를 따라가며
 * to 이하인 모든 키의 offset을 동적으로 모아 돌려준다.
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

    /* 첫 leaf에 도착한 뒤에는 from 이상이 시작되는 위치부터 보면 된다. */
    pos = leaf_lower_bound(leaf, from);

    while (leaf) {
        /* 첫 leaf 이후로 옆 leaf를 넘길 때도 노드 방문 1회로 기록한다. */
        if (!first_leaf)
            record_node_visit(tree);

        while (pos < leaf->key_count) {
            int key = leaf->keys[pos];
            BPValueList *list = leaf->values[pos];

            /* leaf가 정렬되어 있으므로 to를 넘는 순간 전체 탐색을 끝낼 수 있다. */
            if (key > to) {
                *out_count = buffer.count;
                if (buffer.count == 0) {
                    free(buffer.offsets);
                    return NULL;
                }
                return buffer.offsets;
            }

            if (key >= from) {
                /* 같은 key에 매달린 offset들을 모두 결과 버퍼에 펼쳐 넣는다. */
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

        /* 현재 leaf를 다 봤으면 오른쪽 leaf로 이동해서 계속 이어서 본다. */
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

/* 기존 제한형 range API는 제한 없는 결과를 받아 필요한 만큼만 복사한다. */
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

/* 현재 트리 높이를 돌려준다. */
int bptree_height(BPTree *tree) {
    if (!tree) return 0;
    return tree->height;
}

/* 마지막 조회에서 기록한 노드 방문 수(tree_io)를 돌려준다. */
int bptree_last_io(BPTree *tree) {
    if (!tree) return 0;
    return tree->last_io_count;
}

/* 디버깅용 공개 출력 함수다. */
void bptree_print(BPTree *tree) {
    if (!tree || !tree->root) {
        printf("[bptree] NULL\n");
        return;
    }

    printf("[bptree] order=%d height=%d\n", tree->order, tree->height);
    print_node(tree->root, 0);
}
