#ifndef BPTREE_H
#define BPTREE_H

/* =========================================================
 * bptree.h — B+ Tree 모듈 공개 인터페이스
 *
 * 담당자 : 김용 (역할 A)
 * 소유 파일:
 *   include/bptree.h        ← 이 파일 (인터페이스 정의)
 *   src/bptree/bptree.c     ← 구현 파일
 *
 * 다른 팀원에게:
 *   이 헤더를 include 해서 API를 사용할 수 있습니다.
 *   하지만 이 파일과 bptree.c 는 김용만 수정합니다.
 *   API 변경이 필요하면 김용에게 먼저 요청하세요.
 * ========================================================= */

/* ── 디스크 I/O 시뮬레이션 옵션 ─────────────────────────────
 * BPTREE_SIMULATE_IO=1 로 컴파일하면 탐색 시 레벨을 이동할
 * 때마다 DISK_IO_DELAY_US 마이크로초 sleep 을 추가한다.
 * 메모리 기반 B+ 트리에서 높이별 시간 차이를 눈에 보이게 만들
 * 때 사용한다.
 *   예) gcc -DBPTREE_SIMULATE_IO=1 ...
 * ─────────────────────────────────────────────────────────── */
#ifndef BPTREE_SIMULATE_IO
#define BPTREE_SIMULATE_IO 0
#endif

#define DISK_IO_DELAY_US 200  /* 레벨당 200 µs */

/* =========================================================
 * 단일 키 B+ Tree  (key: int,  value: long  파일 오프셋)
 * ========================================================= */
typedef struct BPTree BPTree;

/* 생성 / 소멸 */
BPTree *bptree_create(int order);    /* order: 노드당 최대 자식 수 (≥ 3) */
void    bptree_destroy(BPTree *tree);

/* 삽입 */
int  bptree_insert(BPTree *tree, int key, long value);
     /* 반환: 0 성공, -1 실패 */

/* 탐색 */
long bptree_search(BPTree *tree, int key);
     /* 반환: 파일 오프셋(≥0) 또는 -1(미발견) */

/* 범위 탐색 (BETWEEN from AND to) */
int  bptree_range(BPTree *tree, int from, int to,
                  long *out, int max_count);
     /* 반환: 저장된 오프셋 개수. out 배열은 호출자가 할당한다. */

/* 정보 조회 */
int  bptree_height(BPTree *tree);   /* 현재 트리 높이 */
void bptree_print(BPTree *tree);    /* 디버그용 구조 출력 */

/* =========================================================
 * 복합 키 B+ Tree  (key: (int id, int age),  value: long)
 * 키 비교 기준: key1(id) 먼저, 같으면 key2(age) 비교 (사전순)
 * ========================================================= */
typedef struct BPTreeComp BPTreeComp;

/* 생성 / 소멸 */
BPTreeComp *bptree_comp_create(int order);
void        bptree_comp_destroy(BPTreeComp *tree);

/* 삽입 */
int  bptree_comp_insert(BPTreeComp *tree, int key1, int key2, long value);

/* 탐색 */
long bptree_comp_search(BPTreeComp *tree, int key1, int key2);
     /* 반환: 파일 오프셋(≥0) 또는 -1(미발견) */

/* 정보 조회 */
int  bptree_comp_height(BPTreeComp *tree);

#endif /* BPTREE_H */
