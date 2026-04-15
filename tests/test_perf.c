#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/bptree.h"
#include "../include/interface.h"
#include "../include/index_manager.h"
#include "../src/executor/executor_internal.h"

#define BENCH_RUNS 3
#define BENCH_LABEL_WIDTH 34

/* ?? ??? ???? ?? ??? ??? ???? ????? ?? ?? ????. */
static int utf8_display_width(const char *text) {
    int width = 0;
    const unsigned char *p = (const unsigned char *)text;

    while (*p) {
        if ((*p & 0x80) == 0x00) {
            width += 1;
            p += 1;
        } else if ((*p & 0xE0) == 0xC0) {
            width += 2;
            p += 2;
        } else if ((*p & 0xF0) == 0xE0) {
            width += 2;
            p += 3;
        } else if ((*p & 0xF8) == 0xF0) {
            width += 2;
            p += 4;
        } else {
            width += 1;
            p += 1;
        }
    }

    return width;
}

static void print_label_prefix(const char *label) {
    int pad = BENCH_LABEL_WIDTH - utf8_display_width(label);

    printf("  %s", label);
    while (pad-- > 0)
        putchar(' ');
}

/*
 * test_perf.c
 *
 * 이 벤치는 같은 논리 쿼리를 두 층위에서 따로 측정한다.
 *
 * 1. Raw 층위:
 *    - 인덱스 API 자체
 *    - 직접 구현한 선형 파일 스캔
 *
 * 2. Executor 층위:
 *    - 실제 db_select() 인덱스 경로
 *    - 강제 linear 실행
 *
 * 이렇게 나누는 이유는 비용이 어디서 생기는지 분리해서 보기 위해서다.
 * - 트리 탐색 자체가 느린지
 * - 아니면 이후 row fetch / ResultSet 생성이 느린지
 */

typedef enum {
    TREE_NONE = -1,
    TREE_ID = 0,
    TREE_AGE = 1
} TreeKind;

/* 최종 리포트에 찍히는 벤치 결과 한 줄이다. */
typedef struct {
    double avg_ms;
    int    rows;
    int    tree_h;
    int    tree_io;
} BenchResult;

typedef enum {
    BENCH_ALL = 0,
    BENCH_POINT = 1,
    BENCH_ID_RANGE = 2,
    BENCH_ID_RANGE_LARGE = 3,
    BENCH_AGE_RANGE = 4,
    BENCH_HEIGHT = 5
} BenchCase;

static const char *bench_case_label(BenchCase bench_case) {
    switch (bench_case) {
    case BENCH_POINT:
        return "\uB2E8\uAC74 \uC870\uD68C";
    case BENCH_ID_RANGE:
        return "\uBC94\uC704 \uC870\uD68C(id)";
    case BENCH_ID_RANGE_LARGE:
        return "\uB300\uBC94\uC704 \uC870\uD68C(id)";
    case BENCH_AGE_RANGE:
        return "\uBC94\uC704 \uC870\uD68C(age)";
    case BENCH_HEIGHT:
        return "\uB192\uC774 \uBE44\uAD50";
    case BENCH_ALL:
    default:
        return "\uC804\uCCB4";
    }
}

static int parse_bench_case(const char *name, BenchCase *out) {
    if (!out) return 0;

    if (!name || strcmp(name, "all") == 0) {
        *out = BENCH_ALL;
        return 1;
    }
    if (strcmp(name, "point") == 0) {
        *out = BENCH_POINT;
        return 1;
    }
    if (strcmp(name, "id-range") == 0) {
        *out = BENCH_ID_RANGE;
        return 1;
    }
    if (strcmp(name, "id-range-large") == 0) {
        *out = BENCH_ID_RANGE_LARGE;
        return 1;
    }
    if (strcmp(name, "age-range") == 0) {
        *out = BENCH_AGE_RANGE;
        return 1;
    }
    if (strcmp(name, "height") == 0) {
        *out = BENCH_HEIGHT;
        return 1;
    }

    return 0;
}

static void print_usage(const char *argv0) {
    fprintf(stderr,
            "\uC0AC\uC6A9\uBC95: %s [table] [rows] [all|point|id-range|id-range-large|age-range|height]\n",
            argv0);
}

static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}

/* 테스트용 SelectStmt를 만들 때 쓰는 작은 안전 복사 헬퍼다. */
static void copy_text(char *dst, size_t size, const char *src) {
    if (!dst || size == 0) return;
    if (!src) src = "";

    strncpy(dst, src, size - 1);
    dst[size - 1] = '\0';
}

/* data/{table}.dat 경로를 만든다. */
static void build_data_path(char *buf, size_t size, const char *table) {
    snprintf(buf, size, "data/%s.dat", table);
}

/* 벤치 실행 전에는 data 파일이 이미 준비되어 있어야 한다. */
static int data_file_exists(const char *table) {
    char path[256];
    build_data_path(path, sizeof(path), table);

    FILE *fp = fopen(path, "rb");
    if (!fp) return 0;
    fclose(fp);
    return 1;
}

/* 컬럼 이름으로 스키마 내 위치를 찾는다. */
static int find_column_index(const TableSchema *schema, const char *name) {
    if (!schema || !name) return -1;

    for (int i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, name) == 0)
            return i;
    }
    return -1;
}

/*
 * 파일 한 줄에서 특정 컬럼 값만 꺼낸다.
 *
 * Raw linear 벤치는 ResultSet까지 만들 필요가 없으므로,
 * 가볍게 조건 판정만 하기 위해 이 함수를 사용한다.
 */
static int extract_column_value(const char *line, int col_idx,
                                char *buf, size_t buf_size) {
    if (!line || !buf || buf_size == 0 || col_idx < 0) return 0;

    const char *p = line;
    int current = 0;
    while (*p && current < col_idx) {
        if (*p == '|') current++;
        p++;
    }
    if (current < col_idx) return 0;

    while (*p == ' ') p++;

    size_t i = 0;
    while (*p && *p != '|' && *p != '\n' && *p != '\r' && i < buf_size - 1)
        buf[i++] = *p++;
    while (i > 0 && buf[i - 1] == ' ') i--;
    buf[i] = '\0';
    return 1;
}

/* Raw linear 벤치에서 쓰는 WHERE 조건 판정기다. */
static int raw_line_matches(const char *line, const SelectStmt *stmt,
                            const TableSchema *schema) {
    if (!stmt->has_where) return 1;

    int col_idx = find_column_index(schema, stmt->where.col);
    if (col_idx < 0) return 1;

    char value[256];
    if (!extract_column_value(line, col_idx, value, sizeof(value))) return 0;

    if (stmt->where.type == WHERE_EQ)
        return strcmp(value, stmt->where.val) == 0;

    if (stmt->where.type == WHERE_BETWEEN) {
        if (schema->columns[col_idx].type != COL_INT) return 0;

        int current = atoi(value);
        int from = atoi(stmt->where.val_from);
        int to = atoi(stmt->where.val_to);
        return current >= from && current <= to;
    }

    return 1;
}

/* 쿼리 형태 검증 규칙을 여기서 다시 쓰지 않고 schema_validate를 재사용한다. */
static int validate_select_stmt(const SelectStmt *stmt,
                                const TableSchema *schema) {
    ASTNode node;
    memset(&node, 0, sizeof(node));
    node.type = STMT_SELECT;
    node.select = *stmt;
    return schema_validate(&node, schema);
}

/* 메모리 안에서 SELECT * ... WHERE col = value 형태를 만든다. */
static void init_select_eq(SelectStmt *stmt, const char *table,
                           const char *col, const char *value) {
    memset(stmt, 0, sizeof(*stmt));
    stmt->select_all = 1;
    copy_text(stmt->table, sizeof(stmt->table), table);
    stmt->has_where = 1;
    copy_text(stmt->where.col, sizeof(stmt->where.col), col);
    stmt->where.type = WHERE_EQ;
    copy_text(stmt->where.val, sizeof(stmt->where.val), value);
}

/* 메모리 안에서 SELECT * ... WHERE col BETWEEN from AND to 형태를 만든다. */
static void init_select_between(SelectStmt *stmt, const char *table,
                                const char *col,
                                const char *from, const char *to) {
    memset(stmt, 0, sizeof(*stmt));
    stmt->select_all = 1;
    copy_text(stmt->table, sizeof(stmt->table), table);
    stmt->has_where = 1;
    copy_text(stmt->where.col, sizeof(stmt->where.col), col);
    stmt->where.type = WHERE_BETWEEN;
    copy_text(stmt->where.val_from, sizeof(stmt->where.val_from), from);
    copy_text(stmt->where.val_to, sizeof(stmt->where.val_to), to);
}

/* 벤치 대상 종류에 맞는 트리 높이 조회 함수를 연결한다. */
static int tree_height(const char *table, TreeKind kind) {
    if (kind == TREE_ID) return index_height_id(table);
    if (kind == TREE_AGE) return index_height_age(table);
    return -1;
}

/* 벤치 결과 한 줄을 보기 좋게 출력한다. */
static void print_result_row(const char *label, const BenchResult *result) {
    char tree_buf[32];

    if (result->tree_h >= 0)
        snprintf(tree_buf, sizeof(tree_buf), "%d", result->tree_h);
    else
        copy_text(tree_buf, sizeof(tree_buf), "-");

    print_label_prefix(label);
    printf("\uC2DC\uAC04=%10.3f ms  \uD589\uC218=%-8d  \uD2B8\uB9AC\uB192\uC774=%-3s  \uD2B8\uB9ACI/O=%d\n",
           result->avg_ms, result->rows, tree_buf, result->tree_io);
}

/*
 * 두 경로가 같은 row 수를 돌려줬을 때만 speedup 비율을 출력한다.
 * row 수가 다르면 성능보다 정합성 차이가 더 중요하므로 mismatch를 출력한다.
 */
static void print_speedup_or_mismatch(const char *label,
                                      const BenchResult *index_result,
                                      const BenchResult *linear_result) {
    if (index_result->rows != linear_result->rows) {
        print_label_prefix(label);
        printf("\uACB0\uACFC \uBD88\uC77C\uCE58(\uC778\uB371\uC2A4=%d, \uC120\uD615=%d)\n",
               index_result->rows, linear_result->rows);
        return;
    }

    if (index_result->avg_ms <= 0.0) {
        print_label_prefix(label);
        printf("\uACC4\uC0B0 \uBD88\uAC00\n");
        return;
    }

    print_label_prefix(label);
    printf("%10.2fx\n", linear_result->avg_ms / index_result->avg_ms);
}

static void print_separator(void) {
    printf("  %s\n",
           "-------------------------------------------------------------------"
           "--------------------");
}

/*
 * Raw linear 기준선:
 * data 파일을 직접 처음부터 끝까지 읽으며 조건에 맞는 row 수만 센다.
 *
 * 여기서는 일부러 ResultSet을 만들지 않는다.
 * 목적이 "순수 탐색 / 필터 비용"을 인덱스 API와 비교하는 것이기 때문이다.
 */
static int raw_scan_count(const char *table, const SelectStmt *stmt,
                          const TableSchema *schema, int *rows_out) {
    char path[256];
    build_data_path(path, sizeof(path), table);

    FILE *fp = fopen(path, "rb");
    if (!fp) return SQL_ERR;

    int rows = 0;
    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        /* Raw linear 비교에서는 조건 만족 row 수만 세고 더 무거운 작업은 하지 않는다. */
        if (raw_line_matches(line, stmt, schema))
            rows++;
    }

    fclose(fp);
    *rows_out = rows;
    return SQL_OK;
}

/* Raw 전체 스캔 기준선을 측정한다. */
static int measure_raw_linear(const char *table, const SelectStmt *stmt,
                              const TableSchema *schema, BenchResult *out) {
    double total = 0.0;
    int rows = 0;

    for (int i = 0; i < BENCH_RUNS; i++) {
        double t0 = now_ms();
        if (raw_scan_count(table, stmt, schema, &rows) != SQL_OK)
            return SQL_ERR;
        total += now_ms() - t0;
    }

    out->avg_ms = total / BENCH_RUNS;
    out->rows = rows;
    out->tree_h = TREE_NONE;
    out->tree_io = 0;
    return SQL_OK;
}

/* id point lookup의 순수 인덱스 API 비용만 측정한다. */
static int measure_raw_id_point(const char *table, int target_id,
                                BenchResult *out) {
    double total = 0.0;
    long offset = -1;

    for (int i = 0; i < BENCH_RUNS; i++) {
        /* 이전 실행의 tree_io 흔적이 남지 않도록 매번 초기화한다. */
        index_reset_io_stats(table);
        double t0 = now_ms();
        offset = index_search_id(table, target_id);
        total += now_ms() - t0;
    }

    out->avg_ms = total / BENCH_RUNS;
    out->rows = (offset >= 0) ? 1 : 0;
    out->tree_h = index_height_id(table);
    out->tree_io = index_last_io_id(table);
    return SQL_OK;
}

/* id range lookup의 순수 인덱스 API 비용만 측정한다. */
static int measure_raw_id_range(const char *table, int from, int to,
                                BenchResult *out) {
    double total = 0.0;
    int rows = 0;

    for (int i = 0; i < BENCH_RUNS; i++) {
        index_reset_io_stats(table);
        double t0 = now_ms();
        int count = 0;
        long *offsets = index_range_id_alloc(table, from, to, &count);
        total += now_ms() - t0;
        /* raw 층위에서는 실제 row를 읽지 않고 offset 개수만 결과 row 수로 본다. */
        rows = count;
        free(offsets);
    }

    out->avg_ms = total / BENCH_RUNS;
    out->rows = rows;
    out->tree_h = index_height_id(table);
    out->tree_io = index_last_io_id(table);
    return SQL_OK;
}

/* age range lookup의 순수 인덱스 API 비용만 측정한다. */
static int measure_raw_age_range(const char *table, int from, int to,
                                 BenchResult *out) {
    double total = 0.0;
    int rows = 0;

    for (int i = 0; i < BENCH_RUNS; i++) {
        index_reset_io_stats(table);
        double t0 = now_ms();
        int count = 0;
        long *offsets = index_range_age_alloc(table, from, to, &count);
        total += now_ms() - t0;
        /* age range도 raw 비교에서는 offset 수만 센다. */
        rows = count;
        free(offsets);
    }

    out->avg_ms = total / BENCH_RUNS;
    out->rows = rows;
    out->tree_h = index_height_age(table);
    out->tree_io = index_last_io_age(table);
    return SQL_OK;
}

/*
 * executor 전체 경로를 측정한다.
 * - 경로 선택
 * - offset fetch
 * - row 파싱
 * - ResultSet 생성
 *
 * 즉 사용자가 sqlp를 실행할 때 실제로 체감하는 비용에 더 가깝다.
 */
static int measure_executor_select(const SelectStmt *stmt,
                                   const TableSchema *schema,
                                   int force_linear, TreeKind kind,
                                   BenchResult *out) {
    double total = 0.0;
    int rows = 0;
    SelectExecInfo info = {0};

    for (int i = 0; i < BENCH_RUNS; i++) {
        SelectExecInfo current = {0};
        double t0 = now_ms();
        ResultSet *rs = db_select_mode(stmt, schema, force_linear, 0, &current);
        double elapsed = now_ms() - t0;
        if (!rs) return SQL_ERR;

        /* executor 층위에서는 사용자가 실제로 보게 되는 row_count를 그대로 쓴다. */
        rows = rs->row_count;
        total += elapsed;
        /* 마지막 실행에서 기록된 경로 / tree_io를 리포트용으로 보관한다. */
        info = current;
        result_free(rs);
    }

    out->avg_ms = total / BENCH_RUNS;
    out->rows = rows;
    out->tree_h = force_linear ? TREE_NONE : tree_height(stmt->table, kind);
    out->tree_io = force_linear ? 0 : info.tree_io;
    return SQL_OK;
}

/* 1번 테스트: id 단건 조회 비교 */
static int bench_point_search(const char *table, const TableSchema *schema,
                              int target_id) {
    char id_buf[32];
    SelectStmt stmt;
    BenchResult raw_index;
    BenchResult raw_linear;
    BenchResult exec_index;
    BenchResult exec_linear;

    snprintf(id_buf, sizeof(id_buf), "%d", target_id);
    /* ??? SELECT AST? ?? ???? parser ??? ????. */
    init_select_eq(&stmt, table, "id", id_buf);
    if (validate_select_stmt(&stmt, schema) != SQL_OK) return SQL_ERR;

    if (measure_raw_id_point(table, target_id, &raw_index) != SQL_OK ||
        measure_raw_linear(table, &stmt, schema, &raw_linear) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_ID, &exec_index) != SQL_OK ||
        measure_executor_select(&stmt, schema, 1, TREE_ID, &exec_linear) != SQL_OK)
        return SQL_ERR;

    printf("\n[\uD14C\uC2A4\uD2B8 1] \uB2E8\uAC74 \uC870\uD68C: WHERE id = %d\n", target_id);
    print_separator();
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: \uC778\uB371\uC2A4", &raw_index);
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: \uC120\uD615 \uC2A4\uCE94", &raw_linear);
    print_speedup_or_mismatch("\uD0D0\uC0C9 \uC2DC\uAC04 \uC18D\uB3C4\uBE44\uC728: \uC120\uD615/\uC778\uB371\uC2A4", &raw_index, &raw_linear);
    printf("\n");
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: \uC778\uB371\uC2A4", &exec_index);
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: \uC120\uD615 \uC2A4\uCE94", &exec_linear);
    print_speedup_or_mismatch("\uC2E4\uC81C \uC2E4\uD589 \uC18D\uB3C4\uBE44\uC728: \uC120\uD615/\uC778\uB371\uC2A4", &exec_index, &exec_linear);
    print_separator();
    return SQL_OK;
}

/* 2번 테스트: id 범위 조회 비교 */
static int bench_id_range_search(const char *table, const TableSchema *schema,
                                 int test_no, const char *title,
                                 int from, int to) {
    char from_buf[32];
    char to_buf[32];
    SelectStmt stmt;
    BenchResult raw_index;
    BenchResult raw_linear;
    BenchResult exec_index;
    BenchResult exec_linear;

    snprintf(from_buf, sizeof(from_buf), "%d", from);
    snprintf(to_buf, sizeof(to_buf), "%d", to);
    /* ?? ?? ??? Raw / Executor ??? ???? ?? ?? AST? ?? ???. */
    init_select_between(&stmt, table, "id", from_buf, to_buf);
    if (validate_select_stmt(&stmt, schema) != SQL_OK) return SQL_ERR;

    if (measure_raw_id_range(table, from, to, &raw_index) != SQL_OK ||
        measure_raw_linear(table, &stmt, schema, &raw_linear) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_ID, &exec_index) != SQL_OK ||
        measure_executor_select(&stmt, schema, 1, TREE_ID, &exec_linear) != SQL_OK)
        return SQL_ERR;

    printf("\n[\uD14C\uC2A4\uD2B8 %d] %s: WHERE id BETWEEN %d AND %d\n",
           test_no, title, from, to);
    print_separator();
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: \uC778\uB371\uC2A4", &raw_index);
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: \uC120\uD615 \uC2A4\uCE94", &raw_linear);
    print_speedup_or_mismatch("\uD0D0\uC0C9 \uC2DC\uAC04 \uC18D\uB3C4\uBE44\uC728: \uC120\uD615/\uC778\uB371\uC2A4", &raw_index, &raw_linear);
    printf("\n");
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: \uC778\uB371\uC2A4", &exec_index);
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: \uC120\uD615 \uC2A4\uCE94", &exec_linear);
    print_speedup_or_mismatch("\uC2E4\uC81C \uC2E4\uD589 \uC18D\uB3C4\uBE44\uC728: \uC120\uD615/\uC778\uB371\uC2A4", &exec_index, &exec_linear);
    print_separator();
    return SQL_OK;
}

/* 3번 테스트: age 범위 조회 비교. 결과가 많을 때 보조 인덱스 비용이 드러나는 구간이다. */
static int bench_age_range_search(const char *table, const TableSchema *schema,
                                  int from, int to) {
    char from_buf[32];
    char to_buf[32];
    SelectStmt stmt;
    BenchResult raw_index;
    BenchResult raw_linear;
    BenchResult exec_index;
    BenchResult exec_linear;

    snprintf(from_buf, sizeof(from_buf), "%d", from);
    snprintf(to_buf, sizeof(to_buf), "%d", to);
    /* age ?? ??? ?? ???? ???? ???? ?? ????. */
    init_select_between(&stmt, table, "age", from_buf, to_buf);
    if (validate_select_stmt(&stmt, schema) != SQL_OK) return SQL_ERR;

    if (measure_raw_age_range(table, from, to, &raw_index) != SQL_OK ||
        measure_raw_linear(table, &stmt, schema, &raw_linear) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_AGE, &exec_index) != SQL_OK ||
        measure_executor_select(&stmt, schema, 1, TREE_AGE, &exec_linear) != SQL_OK)
        return SQL_ERR;

    printf("\n[\uD14C\uC2A4\uD2B8 4] \uBC94\uC704 \uC870\uD68C(age): WHERE age BETWEEN %d AND %d\n", from, to);
    print_separator();
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: \uC778\uB371\uC2A4", &raw_index);
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: \uC120\uD615 \uC2A4\uCE94", &raw_linear);
    print_speedup_or_mismatch("\uD0D0\uC0C9 \uC2DC\uAC04 \uC18D\uB3C4\uBE44\uC728: \uC120\uD615/\uC778\uB371\uC2A4", &raw_index, &raw_linear);
    printf("\n");
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: \uC778\uB371\uC2A4", &exec_index);
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: \uC120\uD615 \uC2A4\uCE94", &exec_linear);
    print_speedup_or_mismatch("\uC2E4\uC81C \uC2E4\uD589 \uC18D\uB3C4\uBE44\uC728: \uC120\uD615/\uC778\uB371\uC2A4", &exec_index, &exec_linear);
    print_separator();
    return SQL_OK;
}

/* 높이 비교 테스트를 위해 두 트리를 같은 order로 다시 만든다. */
static int init_index_for_order(const char *table, int order) {
    index_cleanup();
    /* id/age 트리 모두 같은 order로 다시 만들어 높이 차이만 비교한다. */
    return index_init(table, order, order);
}

/*
 * 4번 테스트: 아주 작은 order와 기본 order를 비교한다.
 *
 * 이 테스트의 목적은 "실서비스 튜닝"이 아니라,
 * 트리 높이 차이를 일부러 크게 만들어서
 * 추가 노드 방문이 시간과 tree_io에 어떤 영향을 주는지 보기 위함이다.
 */
static int bench_height_comparison(const char *table, const TableSchema *schema,
                                   int target_id) {
    char id_buf[32];
    SelectStmt stmt;
    BenchResult raw_small;
    BenchResult exec_small;
    BenchResult raw_default;
    BenchResult exec_default;

    snprintf(id_buf, sizeof(id_buf), "%d", target_id);
    init_select_eq(&stmt, table, "id", id_buf);
    if (validate_select_stmt(&stmt, schema) != SQL_OK) return SQL_ERR;

    if (init_index_for_order(table, IDX_ORDER_SMALL) != 0) return SQL_ERR;
    if (measure_raw_id_point(table, target_id, &raw_small) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_ID, &exec_small) != SQL_OK)
        return SQL_ERR;

    /* ?? ???? order? ?? ?? ??? ??? point query? ? ? ? ??. */
    if (init_index_for_order(table, IDX_ORDER_DEFAULT) != 0) return SQL_ERR;
    if (measure_raw_id_point(table, target_id, &raw_default) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_ID, &exec_default) != SQL_OK)
        return SQL_ERR;

    printf("\n[\uD14C\uC2A4\uD2B8 5] \uB192\uC774 \uBE44\uAD50: order=4 vs order=128\n");
    print_separator();
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: order=4", &raw_small);
    print_result_row("\uD0D0\uC0C9 \uC2DC\uAC04: order=128", &raw_default);
    print_speedup_or_mismatch("\uD0D0\uC0C9 \uC2DC\uAC04 \uC18D\uB3C4\uBE44\uC728: order4/order128", &raw_default, &raw_small);
    printf("\n");
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: order=4", &exec_small);
    print_result_row("\uC2E4\uC81C \uC2E4\uD589: order=128", &exec_default);
    print_speedup_or_mismatch("\uC2E4\uC81C \uC2E4\uD589 \uC18D\uB3C4\uBE44\uC728: order4/order128", &exec_default, &exec_small);
    print_separator();
    return init_index_for_order(table, IDX_ORDER_DEFAULT);
}

int main(int argc, char *argv[]) {
    const char *table = (argc > 1) ? argv[1] : "users";
    int rows = (argc > 2) ? atoi(argv[2]) : 1000000;
    const char *case_name = (argc > 3) ? argv[3] : "all";
    int large_range_to = (rows > 0) ? rows : 1;
    BenchCase bench_case = BENCH_ALL;
    TableSchema *schema = NULL;

    if (!parse_bench_case(case_name, &bench_case)) {
        print_usage(argv[0]);
        return 1;
    }

    /* CLI ??? ??? ???, ?? ??? ? ?? row ??. */
    printf("============================================================\n");
    printf("  B+\uD2B8\uB9AC \uC131\uB2A5 \uBCA4\uCE58\uB9C8\uD06C\n");
    printf("  \uD14C\uC774\uBE14: %s  |  \uD589 \uC218: %d\n", table, rows);
#if BPTREE_SIMULATE_IO
    printf("  \uBAA8\uB4DC: B+\uD2B8\uB9AC \uD398\uC774\uC9C0 \uC77D\uAE30 \uC2DC\uBBAC\uB808\uC774\uC158 \uCF1C\uC9D0\n");
#else
    printf("  \uBAA8\uB4DC: \uBA54\uBAA8\uB9AC \uAE30\uBC18 (I/O \uC2DC\uBBAC\uB808\uC774\uC158 \uAEBC\uC9D0)\n");
#endif
    printf("  \uC2E4\uD589 \uD56D\uBAA9: %s\n", bench_case_label(bench_case));
    printf("============================================================\n");

    schema = schema_load(table);
    if (!schema) {
        fprintf(stderr, "schema_load failed for '%s'.\n", table);
        return 1;
    }

    /*
     * test_perf? ??? ???? ?? ???? ???.
     * ?? ???? ???? ?? ?? ??? ??? ??? ?? ???? ????.
     */
    if (!data_file_exists(table)) {
        fprintf(stderr, "\uBCA4\uCE58\uB9C8\uD06C \uB370\uC774\uD130\uAC00 \uC5C6\uC2B5\uB2C8\uB2E4: '%s'\n", table);
        fprintf(stderr, "\uBA3C\uC800 ./sqlp samples/bench_%s.sql \uC744 \uC2E4\uD589\uD558\uC138\uC694.\n", table);
        schema_free(schema);
        return 1;
    }

    printf("\n'%s' \uC778\uB371\uC2A4 \uCD08\uAE30\uD654 \uC911...\n", table);
    index_cleanup();
    if (index_init(table, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0) {
        fprintf(stderr, "index_init \uC2E4\uD328. \uBA3C\uC800 ./sqlp samples/bench_%s.sql \uC744 \uC2E4\uD589\uD558\uC138\uC694.\n",
                table);
        schema_free(schema);
        return 1;
    }

    /* ?? ??? ?? ?? ?? ?? benchmark ??? ???? ????. */
    printf("  \uCD08\uAE30 \uD2B8\uB9AC \uB192\uC774: id=%d  age=%d\n",
           index_height_id(table), index_height_age(table));

    /* ?? ? ??? ?? 1? ?? ??? ??? ???. */
    if (((bench_case == BENCH_ALL || bench_case == BENCH_POINT) &&
         bench_point_search(table, schema, rows / 2) != SQL_OK) ||
        ((bench_case == BENCH_ALL || bench_case == BENCH_ID_RANGE) &&
         bench_id_range_search(table, schema, 2,
                               "\uBC94\uC704 \uC870\uD68C(id)",
                               rows / 4, rows / 2) != SQL_OK) ||
        ((bench_case == BENCH_ALL || bench_case == BENCH_ID_RANGE_LARGE) &&
         bench_id_range_search(table, schema, 3,
                               "\uB300\uBC94\uC704 \uC870\uD68C(id)",
                               1, large_range_to) != SQL_OK) ||
        ((bench_case == BENCH_ALL || bench_case == BENCH_AGE_RANGE) &&
         bench_age_range_search(table, schema, 30, 40) != SQL_OK) ||
        ((bench_case == BENCH_ALL || bench_case == BENCH_HEIGHT) &&
         bench_height_comparison(table, schema, rows / 2) != SQL_OK)) {
        fprintf(stderr, "\uBCA4\uCE58\uB9C8\uD06C \uC2E4\uD589\uC5D0 \uC2E4\uD328\uD588\uC2B5\uB2C8\uB2E4.\n");
        index_cleanup();
        schema_free(schema);
        return 1;
    }

    printf("\n============================================================\n");
    printf("  \uC644\uB8CC\n");
    printf("============================================================\n");

    index_cleanup();
    schema_free(schema);
    return 0;
}
