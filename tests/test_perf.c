#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/bptree.h"
#include "../include/interface.h"
#include "../include/index_manager.h"
#include "../src/executor/executor_internal.h"

#define BENCH_RUNS 3

/*
 * test_perf.c
 *
 * This benchmark intentionally measures the same logical query at two layers:
 *
 * 1. Raw layer:
 *    compare index APIs against a hand-written linear file scan
 *
 * 2. Executor layer:
 *    compare the real db_select() index path against forced linear execution
 *
 * That split is useful because it tells us whether the cost comes from:
 * - the tree lookup itself
 * - or the later row-fetch / ResultSet-building work inside the executor
 */

typedef enum {
    TREE_NONE = -1,
    TREE_ID = 0,
    TREE_AGE = 1
} TreeKind;

/* One benchmark result row shown in the final report. */
typedef struct {
    double avg_ms;
    int    rows;
    int    tree_h;
    int    tree_io;
} BenchResult;

static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}

/* Tiny safe copy helper used when initializing synthetic SelectStmt values. */
static void copy_text(char *dst, size_t size, const char *src) {
    if (!dst || size == 0) return;
    if (!src) src = "";

    strncpy(dst, src, size - 1);
    dst[size - 1] = '\0';
}

/* Build data/{table}.dat. */
static void build_data_path(char *buf, size_t size, const char *table) {
    snprintf(buf, size, "data/%s.dat", table);
}

/* Benchmark setup expects the data file to already exist. */
static int data_file_exists(const char *table) {
    char path[256];
    build_data_path(path, sizeof(path), table);

    FILE *fp = fopen(path, "rb");
    if (!fp) return 0;
    fclose(fp);
    return 1;
}

/* Find a schema column index by name. */
static int find_column_index(const TableSchema *schema, const char *name) {
    if (!schema || !name) return -1;

    for (int i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, name) == 0)
            return i;
    }
    return -1;
}

/*
 * Extract one column value from a serialized row line without building a full
 * Row/ResultSet object. The raw linear benchmark uses this to stay lightweight.
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

/* Predicate evaluator for the raw linear benchmark path. */
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

/* Reuse schema validation instead of duplicating query-shape rules here. */
static int validate_select_stmt(const SelectStmt *stmt,
                                const TableSchema *schema) {
    ASTNode node;
    memset(&node, 0, sizeof(node));
    node.type = STMT_SELECT;
    node.select = *stmt;
    return schema_validate(&node, schema);
}

/* Build a simple SELECT * ... WHERE col = value statement in memory. */
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

/* Build a simple SELECT * ... WHERE col BETWEEN from AND to statement. */
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

/* Map benchmark labels to the right tree height query. */
static int tree_height(const char *table, TreeKind kind) {
    if (kind == TREE_ID) return index_height_id(table);
    if (kind == TREE_AGE) return index_height_age(table);
    return -1;
}

/* Pretty-print one benchmark result row. */
static void print_result_row(const char *label, const BenchResult *result) {
    char tree_buf[32];

    if (result->tree_h >= 0)
        snprintf(tree_buf, sizeof(tree_buf), "%d", result->tree_h);
    else
        copy_text(tree_buf, sizeof(tree_buf), "-");

    printf("  %-35s %10.3f ms  rows=%-8d  tree_h=%-3s  tree_io=%d\n",
           label, result->avg_ms, result->rows, tree_buf, result->tree_io);
}

/*
 * Print a ratio only when both paths returned the same number of rows.
 * If the row counts differ, the mismatch itself is the important signal.
 */
static void print_speedup_or_mismatch(const char *label,
                                      const BenchResult *index_result,
                                      const BenchResult *linear_result) {
    if (index_result->rows != linear_result->rows) {
        printf("  %-35s mismatch (index=%d, linear=%d)\n",
               label, index_result->rows, linear_result->rows);
        return;
    }

    if (index_result->avg_ms <= 0.0) {
        printf("  %-35s n/a\n", label);
        return;
    }

    printf("  %-35s %10.2fx\n",
           label, linear_result->avg_ms / index_result->avg_ms);
}

static void print_separator(void) {
    printf("  %s\n",
           "-------------------------------------------------------------------"
           "--------------------");
}

/*
 * Raw linear baseline:
 * scan the data file directly and count matching rows.
 *
 * This deliberately does not build a ResultSet because the goal here is to
 * compare raw lookup/filter work against raw index lookup work.
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
        if (raw_line_matches(line, stmt, schema))
            rows++;
    }

    fclose(fp);
    *rows_out = rows;
    return SQL_OK;
}

/* Measure the raw full-scan baseline. */
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

/* Measure only the raw id point lookup cost through the index API. */
static int measure_raw_id_point(const char *table, int target_id,
                                BenchResult *out) {
    double total = 0.0;
    long offset = -1;

    for (int i = 0; i < BENCH_RUNS; i++) {
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

/* Measure only the raw id range lookup cost through the index API. */
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
        rows = count;
        free(offsets);
    }

    out->avg_ms = total / BENCH_RUNS;
    out->rows = rows;
    out->tree_h = index_height_id(table);
    out->tree_io = index_last_io_id(table);
    return SQL_OK;
}

/* Measure only the raw age range lookup cost through the index API. */
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
 * Measure the full executor path:
 * - path selection
 * - offset fetches
 * - row parsing
 * - ResultSet creation
 *
 * This is what users actually feel when they run sqlp.
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

        rows = rs->row_count;
        total += elapsed;
        info = current;
        result_free(rs);
    }

    out->avg_ms = total / BENCH_RUNS;
    out->rows = rows;
    out->tree_h = force_linear ? TREE_NONE : tree_height(stmt->table, kind);
    out->tree_io = force_linear ? 0 : info.tree_io;
    return SQL_OK;
}

/* Test 1: point lookup on id. */
static int bench_point_search(const char *table, const TableSchema *schema,
                              int target_id) {
    char id_buf[32];
    SelectStmt stmt;
    BenchResult raw_index;
    BenchResult raw_linear;
    BenchResult exec_index;
    BenchResult exec_linear;

    snprintf(id_buf, sizeof(id_buf), "%d", target_id);
    init_select_eq(&stmt, table, "id", id_buf);
    if (validate_select_stmt(&stmt, schema) != SQL_OK) return SQL_ERR;

    if (measure_raw_id_point(table, target_id, &raw_index) != SQL_OK ||
        measure_raw_linear(table, &stmt, schema, &raw_linear) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_ID, &exec_index) != SQL_OK ||
        measure_executor_select(&stmt, schema, 1, TREE_ID, &exec_linear) != SQL_OK)
        return SQL_ERR;

    printf("\n[Test 1] Point Search: WHERE id = %d\n", target_id);
    print_separator();
    print_result_row("Raw index (id point)", &raw_index);
    print_result_row("Raw linear", &raw_linear);
    print_speedup_or_mismatch("Raw speedup (linear/index)", &raw_index, &raw_linear);
    print_result_row("Executor index (auto)", &exec_index);
    print_result_row("Executor linear (forced)", &exec_linear);
    print_speedup_or_mismatch("Executor speedup (linear/index)",
                              &exec_index, &exec_linear);
    print_separator();
    return SQL_OK;
}

/* Test 2: id range lookup. */
static int bench_id_range_search(const char *table, const TableSchema *schema,
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
    init_select_between(&stmt, table, "id", from_buf, to_buf);
    if (validate_select_stmt(&stmt, schema) != SQL_OK) return SQL_ERR;

    if (measure_raw_id_range(table, from, to, &raw_index) != SQL_OK ||
        measure_raw_linear(table, &stmt, schema, &raw_linear) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_ID, &exec_index) != SQL_OK ||
        measure_executor_select(&stmt, schema, 1, TREE_ID, &exec_linear) != SQL_OK)
        return SQL_ERR;

    printf("\n[Test 2] Range Search (id): WHERE id BETWEEN %d AND %d\n",
           from, to);
    print_separator();
    print_result_row("Raw index (id range)", &raw_index);
    print_result_row("Raw linear", &raw_linear);
    print_speedup_or_mismatch("Raw speedup (linear/index)", &raw_index, &raw_linear);
    print_result_row("Executor index (auto)", &exec_index);
    print_result_row("Executor linear (forced)", &exec_linear);
    print_speedup_or_mismatch("Executor speedup (linear/index)",
                              &exec_index, &exec_linear);
    print_separator();
    return SQL_OK;
}

/* Test 3: age range lookup. This is where large-result secondary-index costs show up. */
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
    init_select_between(&stmt, table, "age", from_buf, to_buf);
    if (validate_select_stmt(&stmt, schema) != SQL_OK) return SQL_ERR;

    if (measure_raw_age_range(table, from, to, &raw_index) != SQL_OK ||
        measure_raw_linear(table, &stmt, schema, &raw_linear) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_AGE, &exec_index) != SQL_OK ||
        measure_executor_select(&stmt, schema, 1, TREE_AGE, &exec_linear) != SQL_OK)
        return SQL_ERR;

    printf("\n[Test 3] Range Search (age): WHERE age BETWEEN %d AND %d\n",
           from, to);
    print_separator();
    print_result_row("Raw index (age range)", &raw_index);
    print_result_row("Raw linear", &raw_linear);
    print_speedup_or_mismatch("Raw speedup (linear/index)", &raw_index, &raw_linear);
    print_result_row("Executor index (auto)", &exec_index);
    print_result_row("Executor linear (forced)", &exec_linear);
    print_speedup_or_mismatch("Executor speedup (linear/index)",
                              &exec_index, &exec_linear);
    print_separator();
    return SQL_OK;
}

/* Rebuild both trees with the same order for the height comparison test. */
static int init_index_for_order(const char *table, int order) {
    index_cleanup();
    return index_init(table, order, order);
}

/*
 * Test 4: compare a very small order against the default order.
 *
 * The point of this test is not "realistic production tuning". It is a way to
 * exaggerate the tree height difference so we can observe how extra node visits
 * change time and tree_io.
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

    if (init_index_for_order(table, IDX_ORDER_DEFAULT) != 0) return SQL_ERR;
    if (measure_raw_id_point(table, target_id, &raw_default) != SQL_OK ||
        measure_executor_select(&stmt, schema, 0, TREE_ID, &exec_default) != SQL_OK)
        return SQL_ERR;

    printf("\n[Test 4] Height Comparison: order=4 vs order=128\n");
    print_separator();
    print_result_row("Raw order=4", &raw_small);
    print_result_row("Raw order=128", &raw_default);
    print_speedup_or_mismatch("Raw speedup (order4/order128)",
                              &raw_default, &raw_small);
    print_result_row("Executor order=4", &exec_small);
    print_result_row("Executor order=128", &exec_default);
    print_speedup_or_mismatch("Executor speedup (order4/order128)",
                              &exec_default, &exec_small);
    print_separator();
    return init_index_for_order(table, IDX_ORDER_DEFAULT);
}

int main(int argc, char *argv[]) {
    const char *table = (argc > 1) ? argv[1] : "users";
    int rows = (argc > 2) ? atoi(argv[2]) : 1000000;
    TableSchema *schema = NULL;

    /* The CLI accepts a table name and the expected row count used in labels. */
    printf("============================================================\n");
    printf("  B+ Tree Performance Benchmark\n");
    printf("  Table: %s  |  Rows: %d\n", table, rows);
#if BPTREE_SIMULATE_IO
    printf("  Mode: B+Tree page-read simulation ON\n");
#else
    printf("  Mode: In-Memory (no I/O simulation)\n");
#endif
    printf("============================================================\n");

    schema = schema_load(table);
    if (!schema) {
        fprintf(stderr, "schema_load failed for '%s'.\n", table);
        return 1;
    }

    /*
     * test_perf does not auto-load data on purpose.
     * We want the benchmark step to measure queries only, not data preparation.
     */
    if (!data_file_exists(table)) {
        fprintf(stderr, "benchmark data missing for '%s'.\n", table);
        fprintf(stderr, "Run ./sqlp samples/bench_%s.sql first.\n", table);
        schema_free(schema);
        return 1;
    }

    printf("\nInitializing index for '%s'...\n", table);
    index_cleanup();
    if (index_init(table, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0) {
        fprintf(stderr, "index_init failed. Run ./sqlp samples/bench_%s.sql first.\n",
                table);
        schema_free(schema);
        return 1;
    }

    printf("  tree_h(id)=%d  tree_h(age)=%d\n",
           index_height_id(table), index_height_age(table));

    /* The four sections below form the full first-stage benchmark report. */
    if (bench_point_search(table, schema, rows / 2) != SQL_OK ||
        bench_id_range_search(table, schema, rows / 4, rows / 2) != SQL_OK ||
        bench_age_range_search(table, schema, 30, 40) != SQL_OK ||
        bench_height_comparison(table, schema, rows / 2) != SQL_OK) {
        fprintf(stderr, "benchmark failed.\n");
        index_cleanup();
        schema_free(schema);
        return 1;
    }

    printf("\n============================================================\n");
    printf("  Done.\n");
    printf("============================================================\n");

    index_cleanup();
    schema_free(schema);
    return 0;
}
