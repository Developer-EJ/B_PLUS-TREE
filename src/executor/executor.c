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

/*
 * executor.c
 *
 * 이 파일은 파서가 만든 AST와 실제 데이터 파일 사이를 이어 주는 실행기다.
 *
 * 핵심 역할:
 * 1. INSERT 실행
 *    - data/{table}.dat 뒤에 새 row를 한 줄 추가한다.
 *    - 그 row가 시작한 파일 offset을 기록한다.
 *    - id / age 인덱스에 같은 offset을 넣는다.
 *
 * 2. SELECT 실행
 *    - 어떤 WHERE 절이 인덱스를 탈 수 있는지 판정한다.
 *    - 인덱스를 타면 offset을 받아 실제 row를 다시 읽는다.
 *    - 인덱스를 못 타면 파일 전체를 선형 탐색한다.
 *
 * 출력 규칙:
 * - stdout: 결과 테이블
 * - stderr: 시간 / 진단 로그
 */

static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}

/* C99 환경에서 strdup 대신 쓰는 간단한 문자열 복사 함수다. */
static char *dup_string(const char *src) {
    if (!src) src = "";

    size_t len = strlen(src) + 1;
    char *copy = (char *)malloc(len);
    if (!copy) return NULL;

    memcpy(copy, src, len);
    return copy;
}

/* 고정 크기 버퍼에 안전하게 문자열을 복사할 때 쓰는 헬퍼다. */
static void copy_text(char *dst, size_t size, const char *src) {
    if (!dst || size == 0) return;
    if (!src) src = "";

    strncpy(dst, src, size - 1);
    dst[size - 1] = '\0';
}

/* 스키마에서 "id", "age" 같은 컬럼 이름의 위치를 찾는다. */
static int find_column_index(const TableSchema *schema, const char *name) {
    if (!schema || !name) return -1;

    for (int i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, name) == 0)
            return i;
    }

    return -1;
}

/*
 * 파일에 저장된 한 줄 row 문자열에서 특정 컬럼 값만 꺼낸다.
 *
 * 예:
 *   "1 | alice | 25 | alice@example.com"
 *
 * 선형 탐색 경로에서는 B+Tree 도움 없이 파일 줄을 직접 검사해야 하므로
 * 이 헬퍼가 WHERE 조건 판정에 쓰인다.
 */
static int line_column_value(const char *line, int col_idx,
                             char *buf, size_t buf_size) {
    if (!line || !buf || buf_size == 0 || col_idx < 0) return 0;

    char tmp[1024];
    strncpy(tmp, line, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';

    /* strtok로 자르기 때문에 원본 line을 훼손하지 않도록 임시 버퍼를 쓴다. */
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

/* 결과 row는 비어 있어도 컬럼 정보는 살아 있는 빈 ResultSet을 만든다. */
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

/*
 * 선형 탐색 경로에서 WHERE col = value 형태를 검사한다.
 *
 * 예:
 *   SELECT * FROM users WHERE name = 'alice'
 *
 * 현재 인덱스 경로는 id / age의 일부 패턴만 처리하므로,
 * 나머지 단순 equality 조건은 결국 이 함수로 내려온다.
 */
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

/*
 * 선형 탐색용 통합 필터 진입점이다.
 *
 * 이 함수 덕분에 강제 linear 실행에서도 아래 조건을 처리할 수 있다.
 * - WHERE id BETWEEN ...
 * - WHERE age BETWEEN ...
 * - 기존 WHERE col = value
 */
static int line_matches_filter(const char *line, const SelectStmt *stmt,
                               const TableSchema *schema) {
    if (!stmt->has_where) return 1;
    if (stmt->where.type == WHERE_EQ)
        return line_matches_where(line, stmt, schema);

    if (stmt->where.type == WHERE_BETWEEN) {
        int col_idx = find_column_index(schema, stmt->where.col);
        if (col_idx < 0) return 1;
        /* BETWEEN은 현재 정수형 컬럼에 대해서만 선형 비교를 지원한다. */
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

/* 파일 한 줄을 Row 구조체 하나로 파싱한다. */
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

/*
 * 이미 열린 테이블 파일에서 조건에 맞는 모든 row를 읽는다.
 *
 * 즉 선형 fallback의 핵심 구현이다.
 * - 파일 처음부터 끝까지 읽고
 * - WHERE 조건을 만족하는 줄만 남기고
 * - Row 배열로 변환해 돌려준다.
 */
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

        /* WHERE 조건에 안 맞는 줄은 바로 건너뛴다. */
        if (!line_matches_filter(line, stmt, schema)) continue;

        if (row_count == capacity) {
            capacity *= 2;
            Row *tmp = (Row *)realloc(rows, (size_t)capacity * sizeof(Row));
            if (!tmp) break;
            rows = tmp;
        }

        /* 조건을 만족한 줄만 실제 Row 구조체로 파싱한다. */
        rows[row_count] = parse_line_to_row(line, schema);
        if (!rows[row_count].values) break;
        row_count++;
    }

    *rows_out = rows;
    return row_count;
}

/*
 * 내부 Row 배열을 외부 공개용 ResultSet 형태로 바꾼다.
 *
 * - SELECT *      : 스키마 순서대로 전체 컬럼 유지
 * - SELECT a,b,c  : 요청된 컬럼만 추려 낸다.
 */
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
            /* projection 대상 컬럼만 골라 새 ResultSet row에 복사한다. */
            rs->rows[r].values[c] = dup_string(
                (si >= 0 && si < rows[r].count) ? rows[r].values[si] : "");
        }
        /* 원본 Row 배열은 ResultSet으로 옮긴 뒤 즉시 정리한다. */
        for (int j = 0; j < rows[r].count; j++)
            free(rows[r].values[j]);
        free(rows[r].values);
    }

    free(rows);
    free(idx);
    return rs;
}

/*
 * 파일 offset 하나를 기준으로 row 한 건을 랜덤 접근해 읽는다.
 *
 * 예:
 *   WHERE id = 5000
 *
 * id 인덱스가 정확한 offset을 알려 주면 executor는 이 함수로
 * 그 위치에 바로 jump 해서 실제 row를 읽는다.
 */
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

    /* offset 위치에서 정확히 한 줄만 읽어 point 조회 결과를 만든다. */
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

/*
 * 여러 개의 offset을 받아 해당 row들을 랜덤 접근으로 읽는다.
 *
 * 중요한 점:
 * - B+Tree는 offset을 빨리 찾는 역할까지만 한다.
 * - 실제 row 내용은 executor가 다시 파일에 가서 읽어 와야 한다.
 *
 * 그래서 secondary index range query는 탐색은 빨라도,
 * 결과 row가 많아지면 선형 탐색보다 느려질 수 있다.
 */
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
        /* offset마다 파일 위치를 다시 옮겨 해당 row를 읽는다. */
        if (fseek(fp, offsets[i], SEEK_SET) != 0) continue;

        char line[1024];
        if (!fgets(line, sizeof(line), fp)) continue;

        int len = (int)strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0) continue;

        /* 읽기에 성공한 row만 실제 결과 배열에 채워 넣는다. */
        rows[actual] = parse_line_to_row(line, schema);
        if (rows[actual].values) actual++;
    }

    fclose(fp);
    return build_resultset(rows, actual, stmt, schema);
}

/* 인덱스를 못 타는 조건이나 강제 linear 모드에서 쓰는 전체 파일 스캔 경로다. */
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

/* compare 모드와 test_perf가 쓰는 실행 결과 요약 구조체를 채운다. */
static void fill_exec_info(SelectExecInfo *info, const char *path,
                           double elapsed_ms, int tree_io, int row_count) {
    if (!info) return;

    copy_text(info->path, sizeof(info->path), path);
    info->elapsed_ms = elapsed_ms;
    info->tree_io = tree_io;
    info->row_count = row_count;
}

/*
 * SELECT의 실제 본체 함수다.
 *
 * 아래 경로들이 모두 이 함수를 공유한다.
 * - 일반 CLI 실행
 * - --force-linear
 * - --compare
 * - 성능 벤치마크
 *
 * 현재 경로 선택 규칙:
 * - WHERE id = ?              -> index:id:eq
 * - WHERE id BETWEEN ? AND ?  -> index:id:range
 * - WHERE age BETWEEN ? AND ? -> index:age:range
 * - 그 외                     -> linear
 *
 * force_linear이 켜지면 인덱스 분기를 건너뛰고 바로 linear로 간다.
 */
ResultSet *db_select_mode(const SelectStmt *stmt, const TableSchema *schema,
                          int force_linear, int emit_log,
                          SelectExecInfo *info) {
    if (!stmt || !schema) return NULL;

    double t0 = now_ms();
    const char *scan_type = "linear";
    int tree_io = 0;
    ResultSet *rs = NULL;

    /* id 조건은 단건 / 범위 둘 다 가장 직접적인 인덱스 경로를 탈 수 있다. */
    if (!force_linear && stmt->has_where && strcmp(stmt->where.col, "id") == 0) {
        if (stmt->where.type == WHERE_EQ) {
            scan_type = "index:id:eq";
            /* 이전 조회의 tree_io가 섞이지 않도록 매 실행 전에 초기화한다. */
            index_reset_io_stats(stmt->table);

            int id = atoi(stmt->where.val);
            long offset = index_search_id(stmt->table, id);
            tree_io = index_last_io_id(stmt->table);
            /* point 조회는 offset 하나만 받아 한 줄만 읽는다. */
            rs = fetch_by_offset(offset, stmt, schema);
        } else if (stmt->where.type == WHERE_BETWEEN) {
            scan_type = "index:id:range";
            index_reset_io_stats(stmt->table);

            int from = atoi(stmt->where.val_from);
            int to = atoi(stmt->where.val_to);
            int count = 0;
            long *offsets = index_range_id_alloc(stmt->table, from, to, &count);
            tree_io = index_last_io_id(stmt->table);
            /* range 조회는 offset 배열을 받아 여러 row를 다시 읽는다. */
            rs = fetch_by_offsets(offsets, count, stmt, schema);
            free(offsets);
        }
    /* age는 현재 범위 조회만 인덱스로 지원한다. */
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
        /* age range도 결국 offset 배열 -> 실제 row 재조회 흐름은 동일하다. */
        rs = fetch_by_offsets(offsets, count, stmt, schema);
        free(offsets);
    }

    /*
     * 지원하지 않는 WHERE 형태는 모두 여기로 떨어진다.
     *
     * 예:
     * - WHERE name = 'alice'
     * - WHERE email = '...'
     * - 강제 linear 비교 / 벤치 실행
     */
    if (!rs) {
        scan_type = "linear";
        tree_io = 0;
        /* 인덱스로 처리하지 못한 모든 경우는 여기서 파일 전체 스캔으로 처리한다. */
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

/* 일반 executor 계약에서 쓰는 공개 SELECT 진입점이다. */
ResultSet *db_select(const SelectStmt *stmt, const TableSchema *schema) {
    return db_select_mode(stmt, schema, 0, 1, NULL);
}

/* 벤치마크에서만 쓰는 조용한 SELECT 진입점이다. */
ResultSet *db_select_bench(const SelectStmt *stmt, const TableSchema *schema,
                           int force_linear) {
    return db_select_mode(stmt, schema, force_linear, 0, NULL);
}

/*
 * row 한 건을 테이블 파일에 추가하고 즉시 인덱스에도 반영한다.
 *
 * 중요한 순서:
 * 1. binary append 모드로 파일을 연다.
 * 2. 쓰기 전에 ftell()로 시작 offset을 잡는다.
 * 3. row를 한 줄로 기록한다.
 * 4. 그 offset을 id / age 인덱스에 넣는다.
 *
 * 나중에 인덱스 SELECT는 이 offset을 row의 "주소"처럼 사용한다.
 */
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

    /* 컬럼 목록이 없으면 입력 값 순서가 이미 스키마 순서라고 본다. */
    if (stmt->column_count == 0) {
        for (int i = 0; i < stmt->value_count; i++) {
            fprintf(fp, "%s", stmt->values[i]);
            if (i < stmt->value_count - 1) fprintf(fp, " | ");
        }
    } else {
        /*
         * 컬럼 목록이 있는 INSERT라면, 실제 파일에는 항상 스키마 순서대로 다시 써 준다.
         * 그래야 INSERT 컬럼 순서가 달라도 data 파일 레이아웃이 흔들리지 않는다.
         */
        for (int s = 0; s < schema->column_count; s++) {
            int val_idx = -1;
            for (int c = 0; c < stmt->column_count; c++) {
                if (strcmp(stmt->columns[c], schema->columns[s].name) == 0) {
                    val_idx = c;
                    break;
                }
            }
            /* 명시되지 않은 컬럼은 빈 문자열로 채워 파일 형식을 유지한다. */
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

    /* id / age 값은 두 가지 INSERT 형태 모두에서 찾아낼 수 있어야 한다. */
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

        /* 실제 원본은 테이블 파일이고, 인덱스는 그 파일을 빠르게 찾기 위한 보조 경로다. */
        index_insert_id(stmt->table, id, offset);
        /* age 값이 있는 경우에만 age 보조 인덱스도 같이 갱신한다. */
        if (age >= 0)
            index_insert_age(stmt->table, age, offset);
    }

    return SQL_OK;
}

/* 기존 인터페이스 호환을 위해 남겨 둔 작은 디스패처다. */
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

/* ResultSet이 들고 있는 모든 heap 메모리를 해제한다. */
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
