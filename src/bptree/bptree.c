#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/bptree.h"

#define IO_PAGE_SIZE 4096
#define IO_SIM_PAGES 256

typedef struct BPValueList {
    long *offsets;
    int   count;
    int   capacity;
} BPValueList;

typedef struct BPNode {
    int is_leaf;
    int key_count;

    int *keys;
    BPValueList **values;
    struct BPNode **children;

    struct BPNode *next;
    struct BPNode *prev;
} BPNode;

typedef struct {
    int     did_split;
    int     promoted_key;
    BPNode *right;
} BPSplitResult;

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

static void record_node_visit(BPTree *tree) {
    if (!tree) return;
    tree->last_io_count++;
    sim_page_read();
}

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

static int internal_child_index(const BPNode *node, int key) {
    int idx = 0;
    while (idx < node->key_count && key >= node->keys[idx])
        idx++;
    return idx;
}

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

static void split_leaf_into_right(BPNode *leaf,
                                  BPNode *right,
                                  BPSplitResult *result) {
    int old_count = leaf->key_count;
    int split_at = old_count / 2;

    for (int i = split_at; i < old_count; i++) {
        int right_idx = right->key_count;
        right->keys[right_idx] = leaf->keys[i];
        right->values[right_idx] = leaf->values[i];
        right->key_count++;
        leaf->values[i] = NULL;
    }

    leaf->key_count = split_at;

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

static int bpnode_insert(BPTree *tree, BPNode *node,
                         int key, long offset, BPSplitResult *result) {
    result->did_split = 0;
    result->right = NULL;

    if (node->is_leaf) {
        int pos = leaf_lower_bound(node, key);
        BPNode *right = NULL;

        if (pos < node->key_count && node->keys[pos] == key)
            return valuelist_insert_sorted(node->values[pos], offset);

        BPValueList *list = valuelist_create(offset);
        if (!list) return -1;

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

    if (bpnode_insert(tree, node->children[child_idx],
                      key, offset, &child_result) != 0)
        return -1;

    if (!child_result.did_split)
        return 0;

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

void bptree_destroy(BPTree *tree) {
    if (!tree) return;
    bpnode_destroy(tree->root);
    free(tree);
}

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

    return leaf->values[pos]->offsets[0];
}

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
