/* =========================================================
 * bptree.c — B+ Tree 구현
 *
 * 담당자 : 김용 (역할 A)
 * 금지   : 다른 팀원은 이 파일을 수정하지 않는다.
 *
 * 구현 체크리스트:
 *   [ ] BPTree 내부 구조체 (BPNode) 정의
 *   [ ] 단일 키: bptree_insert (노드 분열 포함)
 *   [ ] 단일 키: bptree_search (BPTREE_SIMULATE_IO 지원)
 *   [ ] 단일 키: bptree_range  (리프 링크드 리스트 순회)
 *   [ ] 단일 키: bptree_height
 *   [ ] 복합 키: bptree_comp_* (사전순 (key1, key2) 비교)
 *   [ ] bptree_print (디버그 출력)
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
 * 단일 키 B+ Tree 내부 구조
 *
 * TODO (김용): 아래 구조체를 실제 구현에 맞게 채운다.
 *
 * 설계 가이드:
 *   - 내부 노드: keys[order-1] + children[order]
 *   - 리프 노드: keys[order-1] + values[order-1] + next(리프 링크)
 *   - 루트가 리프이면 height = 1
 * ========================================================= */

/* TODO: BPNode 구조체를 여기에 정의한다 */

struct BPTree {
    int  order;   /* 노드당 최대 자식 수 */
    int  height;  /* 현재 트리 높이 (루트=리프이면 1) */
    /* TODO: BPNode *root; 를 추가한다 */
};

/* ── 생성 / 소멸 ─────────────────────────────────────────── */

BPTree *bptree_create(int order) {
    /* TODO: BPTree 와 루트 리프 노드를 할당하고 초기화한다. */
    if (order < 3) order = 3;
    BPTree *t = (BPTree *)calloc(1, sizeof(BPTree));
    if (!t) return NULL;
    t->order  = order;
    t->height = 0;
    return t;
}

void bptree_destroy(BPTree *tree) {
    /* TODO: 모든 노드를 후위 순회하며 free 한다. */
    if (!tree) return;
    free(tree);
}

/* ── 삽입 ────────────────────────────────────────────────── */

int bptree_insert(BPTree *tree, int key, long value) {
    /*
     * TODO (김용):
     *   1. 리프 노드 탐색: 루트부터 내려가며 key 가 들어갈 리프를 찾는다.
     *   2. 리프에 (key, value) 삽입.
     *   3. 리프가 가득 차면 (키 수 == order-1) 분열(split):
     *        - 새 리프 생성, 키를 절반으로 나눔.
     *        - 부모 내부 노드에 구분 키(separator key) 삽입.
     *        - 부모도 가득 차면 재귀적으로 분열.
     *   4. 루트 분열 시 새 루트를 만들고 height++.
     */
    if (!tree) return -1;
    (void)key; (void)value;
    /* 스텁: 항상 성공을 반환. 실제 삽입 미구현. */
    return 0;
}

/* ── 탐색 ────────────────────────────────────────────────── */

long bptree_search(BPTree *tree, int key) {
    /*
     * TODO (김용):
     *   1. 루트부터 내부 노드를 따라 리프까지 내려간다.
     *      각 레벨에서 IO_SLEEP() 을 호출한다.
     *   2. 리프에서 key 를 선형 탐색해 value(파일 오프셋) 반환.
     *   3. 없으면 -1 반환.
     */
    if (!tree) return -1;
    (void)key;
    IO_SLEEP(); /* 스텁에서도 IO 시뮬레이션이 동작하도록 */
    return -1;  /* 스텁: 항상 미발견 */
}

/* ── 범위 탐색 ───────────────────────────────────────────── */

int bptree_range(BPTree *tree, int from, int to,
                 long *out, int max_count) {
    /*
     * TODO (김용):
     *   1. from 에 해당하는 리프까지 내려간다.
     *   2. 리프 링크드 리스트를 따라 key <= to 인 동안 순회한다.
     *   3. 각 매칭 엔트리의 value 를 out[] 에 저장한다.
     *   4. 저장된 개수를 반환한다.
     */
    if (!tree || !out || max_count <= 0) return 0;
    (void)from; (void)to;
    return 0; /* 스텁: 결과 없음 */
}

/* ── 정보 조회 ───────────────────────────────────────────── */

int bptree_height(BPTree *tree) {
    if (!tree) return 0;
    return tree->height;
}

void bptree_print(BPTree *tree) {
    /* TODO (김용): BFS 또는 DFS 로 트리 구조를 출력한다. */
    if (!tree) { printf("[bptree] NULL\n"); return; }
    printf("[bptree] order=%d  height=%d  (stub: 내부 구조 미출력)\n",
           tree->order, tree->height);
}

/* =========================================================
 * 복합 키 B+ Tree 내부 구조
 *
 * 키 비교: key1 (id) 먼저, 같으면 key2 (age) 비교 (사전순).
 * TODO (김용): BPNode 와 동일한 구조를 복합 키 버전으로 확장한다.
 * ========================================================= */

struct BPTreeComp {
    int  order;
    int  height;
    /* TODO: BPNodeComp *root; */
};

/* ── 생성 / 소멸 ─────────────────────────────────────────── */

BPTreeComp *bptree_comp_create(int order) {
    /* TODO: 단일 키와 동일한 패턴으로 복합 키 트리를 초기화한다. */
    if (order < 3) order = 3;
    BPTreeComp *t = (BPTreeComp *)calloc(1, sizeof(BPTreeComp));
    if (!t) return NULL;
    t->order  = order;
    t->height = 0;
    return t;
}

void bptree_comp_destroy(BPTreeComp *tree) {
    /* TODO: 모든 노드를 free 한다. */
    if (!tree) return;
    free(tree);
}

/* ── 삽입 ────────────────────────────────────────────────── */

int bptree_comp_insert(BPTreeComp *tree, int key1, int key2, long value) {
    /*
     * TODO (김용):
     *   단일 키와 동일한 구조. 단, 키 비교 시
     *   (key1_a, key2_a) < (key1_b, key2_b) ⟺
     *     key1_a < key1_b  ||  (key1_a == key1_b && key2_a < key2_b)
     */
    if (!tree) return -1;
    (void)key1; (void)key2; (void)value;
    return 0; /* 스텁 */
}

/* ── 탐색 ────────────────────────────────────────────────── */

long bptree_comp_search(BPTreeComp *tree, int key1, int key2) {
    /*
     * TODO (김용): (key1, key2) 사전순 탐색. 없으면 -1.
     */
    if (!tree) return -1;
    (void)key1; (void)key2;
    IO_SLEEP();
    return -1; /* 스텁 */
}

/* ── 정보 조회 ───────────────────────────────────────────── */

int bptree_comp_height(BPTreeComp *tree) {
    if (!tree) return 0;
    return tree->height;
}
