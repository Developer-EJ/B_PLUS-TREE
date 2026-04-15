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
 * 단일 키 B+ Tree  (key: int, value: long 파일 오프셋)
 *
 * 이 구현체는 두 인덱스 트리에 공통으로 사용된다.
 *   - Tree #1: key=id
 *   - Tree #2: key=age
 *
 * age 트리는 중복 key 를 가질 수 있으므로, 구현은 같은 key 에 대한
 * 여러 엔트리를 안전하게 보관할 수 있어야 한다.
 *
 * 큰 흐름은 아래처럼 이해하면 된다.
 *   1. INSERT:
 *      bptree_insert()
 *        → 내부적으로 리프까지 내려감
 *        → 리프에 key/value 삽입
 *        → overflow 시 split 결과를 부모로 전파
 *        → 루트까지 split 되면 새 루트 생성
 *
 *   2. POINT SEARCH:
 *      bptree_search()
 *        → key 가 들어 있을 리프를 찾음
 *        → 리프에서 lower bound 로 key 위치 확인
 *        → 해당 key 의 대표 offset 1개 반환
 *
 *   3. RANGE SEARCH:
 *      bptree_range()
 *        → from 이 들어갈 첫 리프를 찾음
 *        → 그 리프의 시작 위치부터 검사
 *        → next 링크를 따라 오른쪽 리프들 순회
 *        → key 범위 안의 offset 들을 순서대로 펼쳐 반환
 * ========================================================= */
typedef struct BPTree BPTree;

/* 생성 / 소멸 */
BPTree *bptree_create(int order);    /* order: 노드당 최대 자식 수 (≥ 3) */
void    bptree_destroy(BPTree *tree);

/* 삽입
 * key/value 를 리프에 넣고, 필요하면 split 을 부모로 전파한다.
 * split 이 루트까지 올라오면 트리 높이가 1 증가한다.
 */
int  bptree_insert(BPTree *tree, int key, long value);
     /* 반환: 0 성공, -1 실패 */

/* 탐색
 * point search 는 주로 id 트리에 사용된다.
 * 중복 key 가 있는 age 트리에서는 range search 가 기본 경로다.
 * 내부적으로는 "리프 찾기 → 그 리프 안에서 key 확인" 순서다.
 */
long bptree_search(BPTree *tree, int key);
     /* 반환: 파일 오프셋(≥0) 또는 -1(미발견) */

/* 범위 탐색 (BETWEEN from AND to)
 * from <= key <= to 인 모든 엔트리의 오프셋을 out[] 에 저장한다.
 * 같은 key 가 여러 번 등장하면 그 offset 도 모두 반환 대상이다.
 * 내부적으로는 from 의 시작 리프를 찾은 뒤,
 * 리프 링크드 리스트를 오른쪽으로 따라가며 결과를 모은다.
 * 반환 순서는 항상 다음 규칙을 따른다.
 *   1. key 오름차순
 *   2. 같은 key 안에서는 offset 오름차순
 */
int  bptree_range(BPTree *tree, int from, int to,
                  long *out, int max_count);
     /* 반환: 저장된 오프셋 개수. out 배열은 호출자가 할당한다. */

/* 정보 조회 */
int  bptree_height(BPTree *tree);   /* 현재 트리 높이 */
void bptree_print(BPTree *tree);    /* 디버그용 구조 출력 */

#endif /* BPTREE_H */
