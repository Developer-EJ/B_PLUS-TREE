#ifndef BPTREE_H
#define BPTREE_H

/*
 * Public B+Tree API
 *
 * Tree #1: key=id,  value=file offset
 * Tree #2: key=age, value=file offset
 *
 * When BPTREE_SIMULATE_IO=1 is enabled, node access simulates one fixed-size
 * page read per visited node.
 */

#ifndef BPTREE_SIMULATE_IO
#define BPTREE_SIMULATE_IO 0
#endif

typedef struct BPTree BPTree;

BPTree *bptree_create(int order);
void    bptree_destroy(BPTree *tree);

int  bptree_insert(BPTree *tree, int key, long value);
long bptree_search(BPTree *tree, int key);

int   bptree_range(BPTree *tree, int from, int to, long *out, int max_count);
long *bptree_range_alloc(BPTree *tree, int from, int to, int *out_count);

int  bptree_height(BPTree *tree);
int  bptree_last_io(BPTree *tree);
void bptree_print(BPTree *tree);

#endif /* BPTREE_H */
