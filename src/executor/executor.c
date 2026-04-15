#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#ifdef _WIN32
#  include <direct.h>
#  define MKDIR(p) _mkdir(p)
#else
#  define MKDIR(p) mkdir(p, 0755)
#endif

#include "../../include/interface.h"
#include "../../include/index_manager.h"
#include "executor_internal.h"

static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}

static char *dup_string(const char *src) {
    if (!src) src = "";

    size_t len = strlen(src) + 1;
    char *copy = (char *)malloc(len);
    if (!copy) return NULL;

    memcpy(copy, src, len);
    return copy;
}

static void copy_text(char *dst, size_t size, const char *src) {
    if (!dst || size == 0) return;
    if (!src) src = "";

    strncpy(dst, src, size - 1);
    dst[size - 1] = '\0';
}

static int find_column_index(const TableSchema *schema, const char *name) {
    if (!schema || !name) return -1;

    for (int i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, name) == 0)
            return i;
    }

    return -1;
}

static int line_column_value(const char *line, int col_idx,
                             char *buf, size_t buf_size) {
    if (!line || !buf || buf_size == 0 || col_idx < 0) return 0;

    char tmp[1024];
    strncpy(tmp, line, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';

    char *tok = strtok(tmp, "|");
    for (int i = 0; i < col_idx && tok; i++)
        tok = strtok(NULL, "|");
    if (!tok) return 0;

    while (*tok == ' ') tok++;
    char *end = tok + strlen(tok);
    while (end > tok && end[-1] == ' ') end--;

    size_t len = (size_t)(end - tok);
    if (len >= buf_size) len = buf_size - 1;
    memcpy(buf, tok, len);
    buf[len] = '\0';
    return 1;
}

static ResultSet *make_empty_rs(const TableSchema *schema) {
    ResultSet *rs = (ResultSet *)calloc(1, sizeof(ResultSet));
    if (!rs) return NULL;

    rs->col_count = schema->column_count;
    rs->col_names = (char **)calloc((size_t)rs->col_count, sizeof(char *));
    if (!rs->col_names) {
        free(rs);
        return NULL;
    }

    for (int i = 0; i < rs->col_count; i++)
        rs->col_names[i] = dup_string(schema->columns[i].name);

    return rs;
}

static int line_matches_where(const char *line, const SelectStmt *stmt,
                              const TableSchema *schema) {
    if (!stmt->has_where) return 1;
    if (stmt->where.type != WHERE_EQ) return 1;

    int col_idx = find_column_index(schema, stmt->where.col);
    if (col_idx < 0) return 1;

    char tmp[1024];
    strncpy(tmp, line, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';

    char *tok = strtok(tmp, "|");
    for (int i = 0; i < col_idx && tok; i++)
        tok = strtok(NULL, "|");

    if (tok) {
        while (*tok == ' ') tok++;
        char *end = tok + strlen(tok);
        while (end > tok && end[-1] == ' ') end--;
        *end = '\0';
    }

    return (tok && strcmp(tok, stmt->where.val) == 0) ? 1 : 0;
}

static int line_matches_filter(const char *line, const SelectStmt *stmt,
                               const TableSchema *schema) {
    if (!stmt->has_where) return 1;
    if (stmt->where.type == WHERE_EQ)
        return line_matches_where(line, stmt, schema);

    if (stmt->where.type == WHERE_BETWEEN) {
        int col_idx = find_column_index(schema, stmt->where.col);
        if (col_idx < 0) return 1;
        if (schema->columns[col_idx].type != COL_INT) return 0;

        char value[256];
        if (!line_column_value(line, col_idx, value, sizeof(value))) return 0;

        int current = atoi(value);
        int from = atoi(stmt->where.val_from);
        int to = atoi(stmt->where.val_to);
        return current >= from && current <= to;
    }

    return 1;
}

static Row parse_line_to_row(const char *line, const TableSchema *schema) {
    Row row = {0};
    row.count = schema->column_count;
    row.values = (char **)calloc((size_t)row.count, sizeof(char *));
    if (!row.values) return row;

    char buf[1024];
    strncpy(buf, line, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    char *tok = strtok(buf, "|");
    for (int i = 0; i < row.count; i++) {
        if (tok) {
            while (*tok == ' ') tok++;
            char *end = tok + strlen(tok);
            while (end > tok && end[-1] == ' ') end--;
            *end = '\0';
        }

        row.values[i] = dup_string(tok ? tok : "");
        tok = strtok(NULL, "|");
    }

    return row;
}

static int read_rows(FILE *fp, const SelectStmt *stmt,
                     const TableSchema *schema, Row **rows_out) {
    *rows_out = NULL;

    int capacity = 16;
    int row_count = 0;
    Row *rows = (Row *)calloc((size_t)capacity, sizeof(Row));
    if (!rows) return -1;

    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        int len = (int)strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0) continue;

        if (!line_matches_filter(line, stmt, schema)) continue;

        if (row_count == capacity) {
            capacity *= 2;
            Row *tmp = (Row *)realloc(rows, (size_t)capacity * sizeof(Row));
            if (!tmp) break;
            rows = tmp;
        }

        rows[row_count] = parse_line_to_row(line, schema);
        if (!rows[row_count].values) break;
        row_count++;
    }

    *rows_out = rows;
    return row_count;
}

static ResultSet *build_resultset(Row *rows, int row_count,
                                  const SelectStmt *stmt,
                                  const TableSchema *schema) {
    ResultSet *rs = (ResultSet *)calloc(1, sizeof(ResultSet));
    if (!rs) {
        for (int i = 0; i < row_count; i++) {
            for (int j = 0; j < rows[i].count; j++)
                free(rows[i].values[j]);
            free(rows[i].values);
        }
        free(rows);
        return NULL;
    }

    if (stmt->select_all) {
        rs->col_count = schema->column_count;
        rs->col_names = (char **)calloc((size_t)rs->col_count, sizeof(char *));
        if (!rs->col_names) {
            free(rs);
            return NULL;
        }

        for (int i = 0; i < rs->col_count; i++)
            rs->col_names[i] = dup_string(schema->columns[i].name);
        rs->rows = rows;
        rs->row_count = row_count;
        return rs;
    }

    rs->col_count = stmt->column_count;
    rs->col_names = (char **)calloc((size_t)rs->col_count, sizeof(char *));
    if (!rs->col_names) {
        free(rs);
        return NULL;
    }

    int *idx = (int *)calloc((size_t)rs->col_count, sizeof(int));
    if (!idx) {
        free(rs->col_names);
        free(rs);
        return NULL;
    }

    for (int c = 0; c < stmt->column_count; c++) {
        rs->col_names[c] = dup_string(stmt->columns[c]);
        idx[c] = -1;
        for (int s = 0; s < schema->column_count; s++) {
            if (strcmp(schema->columns[s].name, stmt->columns[c]) == 0) {
                idx[c] = s;
                break;
            }
        }
    }

    rs->rows = (Row *)calloc((size_t)row_count, sizeof(Row));
    if (!rs->rows) {
        free(idx);
        for (int i = 0; i < rs->col_count; i++)
            free(rs->col_names[i]);
        free(rs->col_names);
        free(rs);
        return NULL;
    }

    rs->row_count = row_count;
    for (int r = 0; r < row_count; r++) {
        rs->rows[r].count = rs->col_count;
        rs->rows[r].values = (char **)calloc((size_t)rs->col_count, sizeof(char *));
        for (int c = 0; c < rs->col_count; c++) {
            int si = idx[c];
            rs->rows[r].values[c] = dup_string(
                (si >= 0 && si < rows[r].count) ? rows[r].values[si] : "");
        }
        for (int j = 0; j < rows[r].count; j++)
            free(rows[r].values[j]);
        free(rows[r].values);
    }

    free(rows);
    free(idx);
    return rs;
}

static ResultSet *fetch_by_offset(long offset, const SelectStmt *stmt,
                                  const TableSchema *schema) {
    if (offset < 0) return make_empty_rs(schema);

    char path[256];
    snprintf(path, sizeof(path), "data/%s.dat", stmt->table);

    FILE *fp = fopen(path, "rb");
    if (!fp) return make_empty_rs(schema);

    if (fseek(fp, offset, SEEK_SET) != 0) {
        fclose(fp);
        return make_empty_rs(schema);
    }

    char line[1024];
    if (!fgets(line, sizeof(line), fp)) {
        fclose(fp);
        return make_empty_rs(schema);
    }
    fclose(fp);

    int len = (int)strlen(line);
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
        line[--len] = '\0';
    if (len == 0) return make_empty_rs(schema);

    Row *rows = (Row *)calloc(1, sizeof(Row));
    if (!rows) return NULL;

    rows[0] = parse_line_to_row(line, schema);
    if (!rows[0].values) {
        free(rows);
        return NULL;
    }

    return build_resultset(rows, 1, stmt, schema);
}

static ResultSet *fetch_by_offsets(const long *offsets, int count,
                                   const SelectStmt *stmt,
                                   const TableSchema *schema) {
    if (count <= 0 || !offsets) return make_empty_rs(schema);

    char path[256];
    snprintf(path, sizeof(path), "data/%s.dat", stmt->table);

    FILE *fp = fopen(path, "rb");
    if (!fp) return make_empty_rs(schema);

    Row *rows = (Row *)calloc((size_t)count, sizeof(Row));
    if (!rows) {
        fclose(fp);
        return NULL;
    }

    int actual = 0;
    for (int i = 0; i < count; i++) {
        if (fseek(fp, offsets[i], SEEK_SET) != 0) continue;

        char line[1024];
        if (!fgets(line, sizeof(line), fp)) continue;

        int len = (int)strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0) continue;

        rows[actual] = parse_line_to_row(line, schema);
        if (rows[actual].values) actual++;
    }

    fclose(fp);
    return build_resultset(rows, actual, stmt, schema);
}

static ResultSet *linear_scan(const SelectStmt *stmt,
                              const TableSchema *schema) {
    char path[256];
    snprintf(path, sizeof(path), "data/%s.dat", stmt->table);

    FILE *fp = fopen(path, "rb");
    if (!fp) return make_empty_rs(schema);

    Row *rows = NULL;
    int row_count = read_rows(fp, stmt, schema, &rows);
    fclose(fp);

    if (row_count < 0) return NULL;
    return build_resultset(rows, row_count, stmt, schema);
}

static void fill_exec_info(SelectExecInfo *info, const char *path,
                           double elapsed_ms, int tree_io, int row_count) {
    if (!info) return;

    copy_text(info->path, sizeof(info->path), path);
    info->elapsed_ms = elapsed_ms;
    info->tree_io = tree_io;
    info->row_count = row_count;
}

ResultSet *db_select_mode(const SelectStmt *stmt, const TableSchema *schema,
                          int force_linear, int emit_log,
                          SelectExecInfo *info) {
    if (!stmt || !schema) return NULL;

    double t0 = now_ms();
    const char *scan_type = "linear";
    int tree_io = 0;
    ResultSet *rs = NULL;

    if (!force_linear && stmt->has_where && strcmp(stmt->where.col, "id") == 0) {
        if (stmt->where.type == WHERE_EQ) {
            scan_type = "index:id:eq";
            index_reset_io_stats(stmt->table);

            int id = atoi(stmt->where.val);
            long offset = index_search_id(stmt->table, id);
            tree_io = index_last_io_id(stmt->table);
            rs = fetch_by_offset(offset, stmt, schema);
        } else if (stmt->where.type == WHERE_BETWEEN) {
            scan_type = "index:id:range";
            index_reset_io_stats(stmt->table);

            int from = atoi(stmt->where.val_from);
            int to = atoi(stmt->where.val_to);
            int count = 0;
            long *offsets = index_range_id_alloc(stmt->table, from, to, &count);
            tree_io = index_last_io_id(stmt->table);
            rs = fetch_by_offsets(offsets, count, stmt, schema);
            free(offsets);
        }
    } else if (!force_linear && stmt->has_where &&
               strcmp(stmt->where.col, "age") == 0 &&
               stmt->where.type == WHERE_BETWEEN) {
        scan_type = "index:age:range";
        index_reset_io_stats(stmt->table);

        int from = atoi(stmt->where.val_from);
        int to = atoi(stmt->where.val_to);
        int count = 0;
        long *offsets = index_range_age_alloc(stmt->table, from, to, &count);
        tree_io = index_last_io_age(stmt->table);
        rs = fetch_by_offsets(offsets, count, stmt, schema);
        free(offsets);
    }

    if (!rs) {
        scan_type = "linear";
        tree_io = 0;
        rs = linear_scan(stmt, schema);
    }

    double elapsed = now_ms() - t0;
    int row_count = rs ? rs->row_count : 0;
    fill_exec_info(info, scan_type, elapsed, tree_io, row_count);

    if (emit_log) {
        fprintf(stderr,
                "[SELECT][%-20s] %8.3f ms  tree_h(id)=%d  tree_h(age)=%d  tree_io=%d\n",
                scan_type, elapsed,
                index_height_id(stmt->table),
                index_height_age(stmt->table),
                tree_io);
    }

    return rs;
}

ResultSet *db_select(const SelectStmt *stmt, const TableSchema *schema) {
    return db_select_mode(stmt, schema, 0, 1, NULL);
}

ResultSet *db_select_bench(const SelectStmt *stmt, const TableSchema *schema,
                           int force_linear) {
    return db_select_mode(stmt, schema, force_linear, 0, NULL);
}

int db_insert(const InsertStmt *stmt, const TableSchema *schema) {
    MKDIR("data");

    char path[256];
    snprintf(path, sizeof(path), "data/%s.dat", stmt->table);

    FILE *fp = fopen(path, "ab");
    if (!fp) {
        fprintf(stderr, "executor: cannot open '%s'\n", path);
        return SQL_ERR;
    }

    long offset = ftell(fp);

    if (stmt->column_count == 0) {
        for (int i = 0; i < stmt->value_count; i++) {
            fprintf(fp, "%s", stmt->values[i]);
            if (i < stmt->value_count - 1) fprintf(fp, " | ");
        }
    } else {
        for (int s = 0; s < schema->column_count; s++) {
            int val_idx = -1;
            for (int c = 0; c < stmt->column_count; c++) {
                if (strcmp(stmt->columns[c], schema->columns[s].name) == 0) {
                    val_idx = c;
                    break;
                }
            }
            fprintf(fp, "%s", val_idx >= 0 ? stmt->values[val_idx] : "");
            if (s < schema->column_count - 1) fprintf(fp, " | ");
        }
    }

    fprintf(fp, "\n");
    fclose(fp);

    int id_col = -1;
    int age_col = -1;
    for (int i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, "id") == 0) id_col = i;
        if (strcmp(schema->columns[i].name, "age") == 0) age_col = i;
    }

    const char *id_val = NULL;
    const char *age_val = NULL;

    if (stmt->column_count == 0) {
        if (id_col >= 0 && id_col < stmt->value_count)
            id_val = stmt->values[id_col];
        if (age_col >= 0 && age_col < stmt->value_count)
            age_val = stmt->values[age_col];
    } else {
        for (int c = 0; c < stmt->column_count; c++) {
            if (strcmp(stmt->columns[c], "id") == 0) id_val = stmt->values[c];
            if (strcmp(stmt->columns[c], "age") == 0) age_val = stmt->values[c];
        }
    }

    if (id_val) {
        int id = atoi(id_val);
        int age = age_val ? atoi(age_val) : -1;

        index_insert_id(stmt->table, id, offset);
        if (age >= 0)
            index_insert_age(stmt->table, age, offset);
    }

    return SQL_OK;
}

int executor_run(const ASTNode *node, const TableSchema *schema) {
    if (!node || !schema) return SQL_ERR;

    switch (node->type) {
        case STMT_INSERT:
            if (db_insert(&node->insert, schema) != SQL_OK) return SQL_ERR;
            printf("1 row inserted.\n");
            return SQL_OK;

        case STMT_SELECT: {
            ResultSet *rs = db_select(&node->select, schema);
            if (!rs) return SQL_ERR;
            result_free(rs);
            return SQL_OK;
        }

        default:
            fprintf(stderr, "executor: unknown statement type\n");
            return SQL_ERR;
    }
}

void result_free(ResultSet *rs) {
    if (!rs) return;

    for (int i = 0; i < rs->row_count; i++) {
        for (int j = 0; j < rs->rows[i].count; j++)
            free(rs->rows[i].values[j]);
        free(rs->rows[i].values);
    }

    free(rs->rows);

    for (int i = 0; i < rs->col_count; i++)
        free(rs->col_names[i]);
    free(rs->col_names);

    free(rs);
}
