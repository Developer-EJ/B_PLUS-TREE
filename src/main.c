#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/interface.h"
#include "../include/index_manager.h"
#include "executor/executor_internal.h"

/*
 * main.c
 *
 * sqlp / sqlp_sim의 CLI 진입점이다.
 *
 * 전체 흐름:
 * 1. 명령행 옵션을 읽는다.
 * 2. SQL 파일을 통째로 읽는다.
 * 3. 전체를 토큰화한다.
 * 4. 세미콜론 기준으로 statement를 나눈다.
 * 5. statement마다 파싱 / 검증 / 실행을 수행한다.
 *
 * SELECT는 일반 모드, 강제 linear 모드, compare 모드로 실행될 수 있다.
 */

typedef enum {
    RUN_MODE_AUTO = 0,
    RUN_MODE_FORCE_LINEAR = 1,
    RUN_MODE_COMPARE = 2
} RunMode;

/* ResultSet을 간단한 SQL 클라이언트 스타일 표로 출력한다. */
static void print_pretty_table(ResultSet *rs) {
    if (!rs || rs->row_count == 0) {
        printf("(0 rows)\n");
        return;
    }

    int *widths = (int *)calloc((size_t)rs->col_count, sizeof(int));
    if (!widths) return;

    for (int c = 0; c < rs->col_count; c++) {
        widths[c] = (int)strlen(rs->col_names[c]);
        for (int r = 0; r < rs->row_count; r++) {
            int len = (int)strlen(rs->rows[r].values[c]);
            if (len > widths[c]) widths[c] = len;
        }
    }

#define PRINT_SEP() do { \
    for (int c = 0; c < rs->col_count; c++) { \
        printf("+"); \
        for (int w = 0; w < widths[c] + 2; w++) printf("-"); \
    } \
    printf("+\n"); \
} while (0)

    PRINT_SEP();
    for (int c = 0; c < rs->col_count; c++)
        printf("| %-*s ", widths[c], rs->col_names[c]);
    printf("|\n");
    PRINT_SEP();

    for (int r = 0; r < rs->row_count; r++) {
        for (int c = 0; c < rs->rows[r].count; c++)
            printf("| %-*s ", widths[c], rs->rows[r].values[c]);
        printf("|\n");
    }

    PRINT_SEP();
    printf("(%d row%s)\n", rs->row_count, rs->row_count == 1 ? "" : "s");

    free(widths);
#undef PRINT_SEP
}

/*
 * lexer는 SQL 파일 전체를 한 번에 토큰화한다.
 * 이 함수는 그 토큰 목록을 ';' 기준으로 다시 한 statement씩 잘라 준다.
 */
static TokenList *split_tokens(const TokenList *all, int start,
                               int *next_start) {
    int end = start;
    /* 현재 위치부터 ';' 또는 EOF가 나올 때까지를 한 statement로 본다. */
    while (end < all->count &&
           all->tokens[end].type != TOKEN_SEMICOLON &&
           all->tokens[end].type != TOKEN_EOF)
        end++;

    int token_count = end - start;
    if (token_count == 0) {
        *next_start = (end < all->count &&
                       all->tokens[end].type == TOKEN_SEMICOLON)
                      ? end + 1 : end;
        return NULL;
    }

    TokenList *sub = (TokenList *)malloc(sizeof(TokenList));
    if (!sub) return NULL;

    sub->count = token_count + 1;
    sub->tokens = (Token *)calloc((size_t)sub->count, sizeof(Token));
    if (!sub->tokens) {
        free(sub);
        return NULL;
    }

    for (int i = 0; i < token_count; i++)
        sub->tokens[i] = all->tokens[start + i];

    /* parser는 statement 끝에 EOF가 있기를 기대하므로 가짜 EOF를 붙여 준다. */
    sub->tokens[token_count].type = TOKEN_EOF;
    sub->tokens[token_count].value[0] = '\0';
    sub->tokens[token_count].line =
        all->tokens[end > 0 ? end - 1 : 0].line;

    *next_start = (end < all->count &&
                   all->tokens[end].type == TOKEN_SEMICOLON)
                  ? end + 1 : end;
    return sub;
}

/*
 * compare 모드에서는 결과 테이블은 stdout에 두고,
 * 비교 요약은 stderr 한 줄로만 출력한다.
 *
 * 이렇게 해 두면 화면으로 비교하기도 쉽고,
 * stdout / stderr를 따로 리다이렉션하기도 좋다.
 */
static void print_compare_summary(const SelectExecInfo *auto_info,
                                  const SelectExecInfo *linear_info) {
    if (!auto_info || !linear_info) return;

    if (auto_info->row_count != linear_info->row_count) {
        fprintf(stderr,
                "[COMPARE] auto=%s/%.3fms/%d linear=%s/%.3fms/%d "
                "mismatch(auto=%d, linear=%d)\n",
                auto_info->path, auto_info->elapsed_ms, auto_info->tree_io,
                linear_info->path, linear_info->elapsed_ms, linear_info->tree_io,
                auto_info->row_count, linear_info->row_count);
        return;
    }

    if (auto_info->elapsed_ms <= 0.0) {
        fprintf(stderr,
                "[COMPARE] auto=%s/%.3fms/%d linear=%s/%.3fms/%d speedup=n/a\n",
                auto_info->path, auto_info->elapsed_ms, auto_info->tree_io,
                linear_info->path, linear_info->elapsed_ms, linear_info->tree_io);
        return;
    }

    fprintf(stderr,
            "[COMPARE] auto=%s/%.3fms/%d linear=%s/%.3fms/%d speedup=%.2fx\n",
            auto_info->path, auto_info->elapsed_ms, auto_info->tree_io,
            linear_info->path, linear_info->elapsed_ms, linear_info->tree_io,
            linear_info->elapsed_ms / auto_info->elapsed_ms);
}

/*
 * SELECT 하나를 현재 CLI 모드에 맞춰 실행한다.
 *
 * RUN_MODE_COMPARE는 같은 쿼리를 일부러 두 번 돌린다.
 * - auto 경로 선택
 * - forced linear fallback
 *
 * 그래서 명령 하나만으로 결과 테이블과 시간 차이를 함께 볼 수 있다.
 */
static int run_select(const SelectStmt *stmt, const TableSchema *schema,
                      RunMode mode) {
    if (mode == RUN_MODE_COMPARE) {
        SelectExecInfo auto_info = {0};
        SelectExecInfo linear_info = {0};
        /* 같은 SELECT를 두 번 돌려서 경로 차이만 비교한다. */
        ResultSet *auto_rs = db_select_mode(stmt, schema, 0, 0, &auto_info);
        ResultSet *linear_rs = db_select_mode(stmt, schema, 1, 0, &linear_info);

        if (!auto_rs || !linear_rs) {
            result_free(auto_rs);
            result_free(linear_rs);
            return SQL_ERR;
        }

        /* compare 모드의 출력 순서는 항상 auto -> linear로 고정한다. */
        printf("[AUTO RESULT]\n");
        print_pretty_table(auto_rs);
        printf("[LINEAR RESULT]\n");
        print_pretty_table(linear_rs);
        print_compare_summary(&auto_info, &linear_info);

        result_free(auto_rs);
        result_free(linear_rs);
        return SQL_OK;
    }

    ResultSet *rs = db_select_mode(stmt, schema,
                                   mode == RUN_MODE_FORCE_LINEAR, 1, NULL);
    if (!rs) return SQL_ERR;

    /* 일반 모드와 강제 linear 모드는 결과 테이블 하나만 출력한다. */
    print_pretty_table(rs);
    result_free(rs);
    return SQL_OK;
}

/*
 * SQL statement 하나를 파싱, 검증, 실행한다.
 *
 * 입력 파일에는 여러 statement가 있을 수 있지만,
 * 이 함수는 논리적 statement 하나만 처리한다.
 */
static int run_statement(TokenList *tokens, RunMode mode) {
    ASTNode *ast = parser_parse(tokens);
    if (!ast) {
        fprintf(stderr, "Error: parsing failed\n");
        return SQL_ERR;
    }

    const char *table = (ast->type == STMT_SELECT)
                        ? ast->select.table
                        : ast->insert.table;

    /* 스키마는 검증 기준이면서 결과 테이블 컬럼 구성 기준이기도 하다. */
    TableSchema *schema = schema_load(table);
    if (!schema) {
        fprintf(stderr, "Error: schema not found for table '%s'\n", table);
        parser_free(ast);
        return SQL_ERR;
    }

    if (schema_validate(ast, schema) != SQL_OK) {
        fprintf(stderr, "Error: schema validation failed\n");
        schema_free(schema);
        parser_free(ast);
        return SQL_ERR;
    }

    /*
     * index_init()는 같은 테이블에 대해 멱등적으로 동작하므로,
     * 한 파일 안에서 같은 테이블을 여러 statement가 써도 안전하다.
     */
    if (index_init(table, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0) {
        fprintf(stderr, "Error: index_init failed for table '%s'\n", table);
        schema_free(schema);
        parser_free(ast);
        return SQL_ERR;
    }

    int status = SQL_OK;
    if (ast->type == STMT_SELECT) {
        /* SELECT는 현재 모드에 맞는 경로 선택까지 포함해서 실행한다. */
        status = run_select(&ast->select, schema, mode);
        if (status != SQL_OK)
            fprintf(stderr, "Error: select failed\n");
    } else {
        /* INSERT는 executor가 파일 append + 인덱스 갱신까지 맡는다. */
        status = db_insert(&ast->insert, schema);
        if (status != SQL_OK) {
            fprintf(stderr, "Error: insert failed\n");
        } else {
            printf("1 row inserted.\n");
        }
    }

    schema_free(schema);
    parser_free(ast);
    return status;
}

/* 옵션 파싱 에러에서 공통으로 쓰는 usage 출력이다. */
static void print_usage(const char *argv0) {
    fprintf(stderr, "Usage: %s [--force-linear | --compare] <sql_file>\n", argv0);
}

int main(int argc, char *argv[]) {
    int force_linear = 0;
    int compare = 0;
    const char *sql_path = NULL;

    /*
     * 지원 형태:
     *   ./sqlp file.sql
     *   ./sqlp --force-linear file.sql
     *   ./sqlp --compare file.sql
     */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--force-linear") == 0) {
            force_linear = 1;
        } else if (strcmp(argv[i], "--compare") == 0) {
            compare = 1;
        } else if (argv[i][0] == '-') {
            print_usage(argv[0]);
            return 1;
        } else if (!sql_path) {
            sql_path = argv[i];
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    if (!sql_path || (force_linear && compare)) {
        print_usage(argv[0]);
        return 1;
    }

    /* --force-linear 과 --compare 는 일부러 동시에 못 쓰게 막아 둔다. */
    RunMode mode = RUN_MODE_AUTO;
    if (compare) mode = RUN_MODE_COMPARE;
    else if (force_linear) mode = RUN_MODE_FORCE_LINEAR;

    char *sql = input_read_file(sql_path);
    if (!sql) {
        fprintf(stderr, "Error: cannot open '%s'\n", sql_path);
        return 1;
    }

    /* 먼저 파일 전체를 토큰화하고, 아래에서 statement 단위로 다시 나눈다. */
    TokenList *all_tokens = lexer_tokenize(sql);
    free(sql);
    if (!all_tokens) {
        fprintf(stderr, "Error: tokenization failed\n");
        return 1;
    }

    int total = 0;
    int fail = 0;
    int pos = 0;

    /* 전체 토큰 목록을 statement 단위로 순회한다. */
    while (pos < all_tokens->count) {
        if (all_tokens->tokens[pos].type == TOKEN_EOF) break;

        int next = 0;
        TokenList *sub = split_tokens(all_tokens, pos, &next);
        if (sub) {
            total++;
            /* statement 하나라도 실패하면 전체 종료 코드는 실패가 되도록 fail을 센다. */
            if (run_statement(sub, mode) != SQL_OK) fail++;
            lexer_free(sub);
        }
        /* 다음 statement의 시작 위치로 이동한다. */
        pos = next;
    }

    lexer_free(all_tokens);

    /* 여러 statement가 있었을 때만 요약 문구를 따로 출력한다. */
    if (total > 1) {
        printf("\n%d statement(s) executed", total);
        if (fail > 0) printf(", %d failed", fail);
        printf(".\n");
    }

    /* 실행 중 만들어 둔 메모리 인덱스를 모두 정리한다. */
    index_cleanup();
    return fail > 0 ? 1 : 0;
}
