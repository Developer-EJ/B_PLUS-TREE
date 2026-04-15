/* =========================================================
 * index_manager.c — 인덱스 매니저 구현
 *
 * 담당자 : 김은재 (역할 B)
 * 금지   : 다른 팀원은 이 파일을 수정하지 않는다.
 *
 * 구현 체크리스트:
 *   [ ] index_init: data/{table}.dat 스캔 → 두 트리 동시 구축
 *       - ftell()로 각 행의 시작 오프셋 기록
 *       - 첫 번째 컬럼(id) 파싱 → tree_id 에 삽입
 *       - id + age 컬럼 파싱 → tree_comp 에 삽입
 *   [ ] index_insert_id / index_search_id / index_range_id
 *   [ ] index_insert_comp / index_search_comp
 *   [ ] index_height_id / index_height_comp
 *   [ ] index_cleanup: 모든 트리 해제
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../include/interface.h"
#include "../../include/index_manager.h"
#include "../../include/bptree.h"

/* =========================================================
 * 내부 상태: 테이블별 인덱스 레코드
 * ========================================================= */
typedef struct {
    char        table[64];
    BPTree     *tree_id;    /* Tree #1: id 단일 인덱스       */
    BPTreeComp *tree_comp;  /* Tree #2: (id, age) 복합 인덱스 */
    int         initialized;
} TableIndex;

static TableIndex g_tables[IDX_MAX_TABLES];
static int        g_count = 0;

/* ── 내부 헬퍼: 테이블 이름으로 인덱스 레코드를 찾는다 ── */
static TableIndex *find_entry(const char *table) {
    for (int i = 0; i < g_count; i++) {
        if (strcmp(g_tables[i].table, table) == 0)
            return &g_tables[i];
    }
    return NULL;
}

/* =========================================================
 * index_init
 * ========================================================= */
int index_init(const char *table, int order_id, int order_comp) {
    if (!table || g_count >= IDX_MAX_TABLES) return -1;

    /* 이미 초기화된 테이블이면 즉시 성공 반환 (멱등) */
    if (find_entry(table)) return 0;

    TableIndex *ti = &g_tables[g_count];
    memset(ti, 0, sizeof(TableIndex));
    strncpy(ti->table, table, sizeof(ti->table) - 1);

    int oid  = (order_id   > 2) ? order_id   : IDX_ORDER_DEFAULT;
    int ocmp = (order_comp > 2) ? order_comp : IDX_ORDER_DEFAULT;

    ti->tree_id   = bptree_create(oid);
    ti->tree_comp = bptree_comp_create(ocmp);
    if (!ti->tree_id || !ti->tree_comp) {
        bptree_destroy(ti->tree_id);
        bptree_comp_destroy(ti->tree_comp);
        return -1;
    }

    g_count++;

    /*
     * TODO (김은재): data/{table}.dat 파일을 한 줄씩 읽어
     *   두 트리를 구축한다.
     *
     *   구현 가이드:
     *     FILE *fp = fopen(path, "rb");  ← binary mode (오프셋 정확도)
     *     while (1) {
     *         long offset = ftell(fp);   ← 이 행의 시작 오프셋
     *         if (!fgets(line, ...)) break;
     *
     *         int id  = atoi(col_value(line, 0));  ← 0번 컬럼 = id
     *         int age = atoi(col_value(line, 2));  ← 2번 컬럼 = age
     *
     *         bptree_insert(ti->tree_id, id, offset);
     *         bptree_comp_insert(ti->tree_comp, id, age, offset);
     *     }
     *
     *   컬럼 파싱: '|' 구분자 기준 i 번째 토큰을 반환하는
     *             내부 헬퍼 함수(static)를 만들어 재사용한다.
     */

    fprintf(stderr,
            "[index] '%s' 초기화 (order_id=%d, order_comp=%d) "
            "— 스텁: 파일 스캔 미구현\n",
            table, oid, ocmp);

    ti->initialized = 1;
    return 0;
}

/* =========================================================
 * index_cleanup
 * ========================================================= */
void index_cleanup(void) {
    /* TODO (김은재): 루프 안에서 bptree_destroy / bptree_comp_destroy 호출 */
    for (int i = 0; i < g_count; i++) {
        bptree_destroy(g_tables[i].tree_id);
        bptree_comp_destroy(g_tables[i].tree_comp);
        g_tables[i].initialized = 0;
    }
    g_count = 0;
}

/* =========================================================
 * Tree #1 — id 단일 인덱스
 * ========================================================= */
int index_insert_id(const char *table, int id, long offset) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;
    return bptree_insert(ti->tree_id, id, offset);
}

long index_search_id(const char *table, int id) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;
    return bptree_search(ti->tree_id, id);
}

int index_range_id(const char *table, int from, int to,
                   long *offsets, int max) {
    TableIndex *ti = find_entry(table);
    if (!ti || !offsets) return 0;
    return bptree_range(ti->tree_id, from, to, offsets, max);
}

/* =========================================================
 * Tree #2 — (id, age) 복합 인덱스
 * ========================================================= */
int index_insert_comp(const char *table, int id, int age, long offset) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;
    return bptree_comp_insert(ti->tree_comp, id, age, offset);
}

long index_search_comp(const char *table, int id, int age) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;
    return bptree_comp_search(ti->tree_comp, id, age);
}

/* =========================================================
 * 높이 조회
 * ========================================================= */
int index_height_id(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return bptree_height(ti->tree_id);
}

int index_height_comp(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return bptree_comp_height(ti->tree_comp);
}
