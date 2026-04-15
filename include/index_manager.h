#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

/* =========================================================
 * index_manager.h — 인덱스 매니저 공개 인터페이스
 *
 * 담당자 : 김은재 (역할 B)
 * 소유 파일:
 *   include/index_manager.h      ← 이 파일 (인터페이스 정의)
 *   src/index/index_manager.c    ← 구현 파일
 *
 * 다른 팀원에게:
 *   이 헤더를 include 해서 API를 사용할 수 있습니다.
 *   하지만 이 파일과 index_manager.c 는 김은재만 수정합니다.
 *   API 변경이 필요하면 김은재에게 먼저 요청하세요.
 *
 * 의존성:
 *   내부적으로 bptree.h (김용)를 사용한다.
 *   호출자는 bptree.h 를 직접 include 할 필요 없다.
 * ========================================================= */

#define IDX_ORDER_DEFAULT  128   /* 기본 B+ Tree order (높이 최소화) */
#define IDX_ORDER_SMALL      4   /* 높이 비교 실험용 소형 order     */
#define IDX_MAX_RANGE    65536   /* range query 최대 결과 수         */
#define IDX_MAX_TABLES       8   /* 동시에 관리 가능한 테이블 수    */

/* =========================================================
 * 초기화 / 정리
 * ========================================================= */

/* index_init
 *   테이블의 인덱스를 초기화한다.
 *   이미 초기화된 테이블이면 즉시 성공을 반환한다 (멱등).
 *   내부적으로 data/{table}.dat 를 전체 스캔해
 *   Tree #1 (id) 과 Tree #2 (id, age) 를 구축한다.
 *
 *   order_id   : Tree #1 의 order (IDX_ORDER_DEFAULT 권장)
 *   order_comp : Tree #2 의 order (IDX_ORDER_DEFAULT 권장)
 *   반환: 0 성공, -1 실패
 */
int  index_init(const char *table, int order_id, int order_comp);

/* index_cleanup
 *   모든 테이블의 두 트리를 메모리에서 해제한다.
 *   프로그램 종료 직전에 한 번 호출한다.
 */
void index_cleanup(void);

/* =========================================================
 * Tree #1 — id 단일 인덱스
 * ========================================================= */

/* 삽입: (id, 파일 오프셋)을 Tree #1 에 추가한다. */
int  index_insert_id(const char *table, int id, long offset);

/* 탐색: id 에 해당하는 파일 오프셋을 반환한다. 없으면 -1. */
long index_search_id(const char *table, int id);

/* 범위 탐색: from <= id <= to 인 오프셋을 offsets[] 에 저장한다.
 * 반환값: 저장된 개수. max 는 IDX_MAX_RANGE 이하로 지정한다.  */
int  index_range_id(const char *table, int from, int to,
                    long *offsets, int max);

/* =========================================================
 * Tree #2 — (id, age) 복합 인덱스
 * ========================================================= */

/* 삽입: (id, age, 파일 오프셋)을 Tree #2 에 추가한다. */
int  index_insert_comp(const char *table, int id, int age, long offset);

/* 탐색: (id, age) 에 해당하는 파일 오프셋을 반환한다. 없으면 -1. */
long index_search_comp(const char *table, int id, int age);

/* =========================================================
 * 높이 조회 (성능 비교 출력용)
 * ========================================================= */
int  index_height_id(const char *table);    /* Tree #1 현재 높이 */
int  index_height_comp(const char *table);  /* Tree #2 현재 높이 */

#endif /* INDEX_MANAGER_H */
