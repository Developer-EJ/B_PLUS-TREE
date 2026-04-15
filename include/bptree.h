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

/* ── I/O 시뮬레이션 옵션 ───────────────────────────────────
 * BPTREE_SIMULATE_IO=1 로 컴파일하면,
 * 탐색 중 "노드 1개 방문 = 고정 크기 페이지 1회 읽기" 를 흉내 낸다.
 *
 * 이전처럼 usleep 기반 지연을 넣는 방식이 아니라,
 * 내부적으로 임시 파일에서 4KB 페이지를 읽으며
 * 노드 접근 비용 차이를 비교할 수 있게 만든 버전이다.
 * ─────────────────────────────────────────────────────────── */
#ifndef BPTREE_SIMULATE_IO
#define BPTREE_SIMULATE_IO 0
#endif

/* =========================================================
 * 단일 키 B+ Tree  (key: int, value: long 파일 오프셋)
 *
 * 이 구현체는 두 인덱스 트리에 공통으로 사용된다.
 *   - Tree #1: key=id
 *   - Tree #2: key=age
 *
 * age 트리는 중복 key 를 가질 수 있으므로, 구현은 같은 key 에 대한
 * 여러 offset 을 하나의 key 엔트리 아래 안전하게 보관할 수 있어야 한다.
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
BPTree *bptree_create(int order);    /* order: 노드당 최대 자식 수 (>= 3) */
void    bptree_destroy(BPTree *tree);

/* 삽입
 * key/value 를 리프에 넣고, 필요하면 split 을 부모로 전파한다.
 * split 이 루트까지 올라오면 트리 높이가 1 증가한다.
 */
int  bptree_insert(BPTree *tree, int key, long value);
     /* 반환: 0 성공, -1 실패 */

/* 탐색
 * point search 는 주로 id 트리에 사용된다.
 * 중복 key 가 있더라도 대표 offset 하나를 반환한다.
 */
long bptree_search(BPTree *tree, int key);
     /* 반환: 파일 오프셋(>=0) 또는 -1(미발견) */

/* 범위 탐색 (BETWEEN from AND to)
 * from <= key <= to 인 모든 엔트리의 오프셋을 out[] 에 저장한다.
 * 같은 key 가 여러 번 등장하면 그 offset 도 모두 반환 대상이다.
 * 반환 순서는 항상:
 *   1. key 오름차순
 *   2. 같은 key 안에서는 offset 오름차순
 *
 * 주의:
 *   이 API 는 호출자가 준비한 out[] 크기만큼만 복사한다.
 *   더 큰 범위를 "전부" 받고 싶으면 bptree_range_alloc() 을 사용한다.
 */
int  bptree_range(BPTree *tree, int from, int to,
                  long *out, int max_count);

/* 전체 범위 결과를 동적 할당 버퍼로 반환한다.
 * out_count 에 실제 개수가 기록되며,
 * 반환된 배열은 호출자가 free 해야 한다.
 */
long *bptree_range_alloc(BPTree *tree, int from, int to, int *out_count);

/* 정보 조회 */
int  bptree_height(BPTree *tree);    /* 현재 트리 높이 */
int  bptree_last_io(BPTree *tree);   /* 마지막 search/range 의 노드 방문 수 */
void bptree_print(BPTree *tree);     /* 디버그용 구조 출력 */

#endif /* BPTREE_H */
