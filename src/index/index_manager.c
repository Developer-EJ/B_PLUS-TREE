#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/interface.h"
#include "../../include/index_manager.h"
#include "../../include/bptree.h"

typedef struct {
    char    table[64];
    BPTree *tree_id;
    BPTree *tree_age;
    int     initialized;
    int     last_io_id;
    int     last_io_age;
} TableIndex;

static TableIndex g_tables[IDX_MAX_TABLES];
static int        g_count = 0;

static TableIndex *find_entry(const char *table) {
    for (int i = 0; i < g_count; i++) {
        if (strcmp(g_tables[i].table, table) == 0)
            return &g_tables[i];
    }
    return NULL;
}

static int col_value(const char *line, int n, char *buf, int buf_size) {
    const char *p = line;
    int col = 0;

    while (*p && col < n) {
        if (*p == '|') col++;
        p++;
    }
    if (col < n) return 0;

    while (*p == ' ') p++;

    int i = 0;
    while (*p && *p != '|' && *p != '\n' && *p != '\r' && i < buf_size - 1)
        buf[i++] = *p++;

    while (i > 0 && buf[i - 1] == ' ') i--;
    buf[i] = '\0';
    return 1;
}

int index_init(const char *table, int order_id, int order_age) {
    if (!table || g_count >= IDX_MAX_TABLES) return -1;

    if (find_entry(table)) return 0;

    TableIndex *ti = &g_tables[g_count];
    memset(ti, 0, sizeof(*ti));
    strncpy(ti->table, table, sizeof(ti->table) - 1);

    int oid = (order_id > 2) ? order_id : IDX_ORDER_DEFAULT;
    int oage = (order_age > 2) ? order_age : IDX_ORDER_DEFAULT;

    ti->tree_id = bptree_create(oid);
    ti->tree_age = bptree_create(oage);
    if (!ti->tree_id || !ti->tree_age) {
        bptree_destroy(ti->tree_id);
        bptree_destroy(ti->tree_age);
        return -1;
    }

    g_count++;

    char path[256];
    // 열어야 할 파일 경로를 만든다.
    snprintf(path, sizeof(path), "data/%s.dat", table);

    // binary 모드로 파일을 읽는다.
    // text 모드는 OS가 파일을 읽으면서 개행문자를 자동으로 변환하기 때문에 BYTE 수가 달라진다.
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "[index] '%s' \uCD08\uAE30\uD654 \uC644\uB8CC (\uD30C\uC77C \uC5C6\uC74C, \uBE48 \uC778\uB371\uC2A4)\n",
                table);
        ti->initialized = 1;
        return 0;
    }

    char line[1024];
    char col_buf[64];
    int inserted = 0;

    // 파일을 끝까지 한 줄씩 읽는 무한루프
    while (1) {
        // 1. ftell() -> 이 줄의 시작 offset 기록
        long offset = ftell(fp);
        // 2. fgets() -> 한 줄 읽기(파일 끝이면 break)
        if (!fgets(line, sizeof(line), fp)) break;

        // 3. 줄 끝 개행 제거 후 빈 줄 건너뜀
        int len = (int)strlen(line);
        while (len > 0 &&
               (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0) continue;

        // 4-1. id 파싱
        if (!col_value(line, 0, col_buf, sizeof(col_buf))) continue;
        int id = atoi(col_buf);

        // 4-2. age 파싱
        if (!col_value(line, 2, col_buf, sizeof(col_buf))) continue;
        int age = atoi(col_buf);

        // 5. bptree_insert()로 두 트리에 삽입
        bptree_insert(ti->tree_id, id, offset);
        bptree_insert(ti->tree_age, age, offset);
        // 6. 삽입 횟수 추가
        inserted++;
    }

    fclose(fp);

    fprintf(stderr,
            "[index] '%s' \uCD08\uAE30\uD654 \uC644\uB8CC - %d\uD589 \uB85C\uB4DC "
            "(order_id=%d, order_age=%d)\n",
            table, inserted, oid, oage);

    ti->initialized = 1;
    return 0;
}

void index_cleanup(void) {
    for (int i = 0; i < g_count; i++) {
        bptree_destroy(g_tables[i].tree_id);
        bptree_destroy(g_tables[i].tree_age);
        memset(&g_tables[i], 0, sizeof(g_tables[i]));
    }
    g_count = 0;
}

/* =========================================================
 * Tree #1 — id 단일 인덱스
 * ========================================================= */

/* INSERT 시 호출: (id, offset)을 Tree #1에 삽입한다.
 * executor가 .dat 파일에 쓰고 난 뒤 offset을 넘겨준다. */
int index_insert_id(const char *table, int id, long offset) {
    TableIndex *ti = find_entry(table); /* 테이블 이름으로 TableIndex 찾기 */
    if (!ti) return -1;                 /* 초기화 안 된 테이블이면 실패 */
    return bptree_insert(ti->tree_id, id, offset);
}

/* WHERE id = ? 쿼리: id에 해당하는 offset을 반환한다.
 * 없으면 -1. executor가 반환받은 offset으로 fseek()한다. */
long index_search_id(const char *table, int id) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;

    long offset = bptree_search(ti->tree_id, id);
    ti->last_io_id = bptree_last_io(ti->tree_id);
    return offset;
}

long *index_range_id_alloc(const char *table, int from, int to,
                           int *out_count) {
    TableIndex *ti = find_entry(table);
    if (out_count) *out_count = 0;
    if (!ti || !out_count) return NULL;

    long *offsets = bptree_range_alloc(ti->tree_id, from, to, out_count);
    ti->last_io_id = bptree_last_io(ti->tree_id);
    return offsets;
}

/* WHERE id BETWEEN from AND to 쿼리: 범위 내 모든 offset을 offsets[]에 담는다.
 * 반환값은 담긴 개수. executor가 각 offset에 fseek()해서 행을 읽는다. */
int index_range_id(const char *table, int from, int to,
                   long *offsets, int max) {
    if (!offsets || max <= 0) return 0;

    int count = 0;
    long *all_offsets = index_range_id_alloc(table, from, to, &count);
    if (!all_offsets || count <= 0) return 0;

    int copied = (count < max) ? count : max;
    memcpy(offsets, all_offsets, (size_t)copied * sizeof(long));
    free(all_offsets);
    return copied;
}

/* =========================================================
 * Tree #2 — age 단일 인덱스
 *   age 는 유일하지 않으므로 range search 만 제공한다.
 * ========================================================= */

/* INSERT 시 호출: (age, offset)을 Tree #2에 삽입한다.
 * index_insert_id()와 같은 시점에 executor가 함께 호출한다. */
int index_insert_age(const char *table, int age, long offset) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;
    return bptree_insert(ti->tree_age, age, offset);
}

long *index_range_age_alloc(const char *table, int from, int to,
                            int *out_count) {
    TableIndex *ti = find_entry(table);
    if (out_count) *out_count = 0;
    if (!ti || !out_count) return NULL;

    long *offsets = bptree_range_alloc(ti->tree_age, from, to, out_count);
    ti->last_io_age = bptree_last_io(ti->tree_age);
    return offsets;
}

/* WHERE age BETWEEN from AND to 쿼리: 범위 내 모든 offset을 offsets[]에 담는다.
 * age는 중복 가능하므로 같은 age 값에 여러 offset이 담길 수 있다. */
int index_range_age(const char *table, int from, int to,
                    long *offsets, int max) {
    if (!offsets || max <= 0) return 0;

    int count = 0;
    long *all_offsets = index_range_age_alloc(table, from, to, &count);
    if (!all_offsets || count <= 0) return 0;

    int copied = (count < max) ? count : max;
    memcpy(offsets, all_offsets, (size_t)copied * sizeof(long));
    free(all_offsets);
    return copied;
}

/* =========================================================
 * 높이 조회
 * ========================================================= */

/* Tree #1 현재 높이 반환. order가 클수록 높이가 낮다.
 * make sim 실행 시 성능 비교 출력에 사용된다. */
int index_height_id(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return bptree_height(ti->tree_id);
}

/* Tree #2 현재 높이 반환. */
int index_height_age(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return bptree_height(ti->tree_age);
}

void index_reset_io_stats(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return;

    ti->last_io_id = 0;
    ti->last_io_age = 0;
}

int index_last_io_id(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return ti->last_io_id;
}

int index_last_io_age(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return ti->last_io_age;
}
