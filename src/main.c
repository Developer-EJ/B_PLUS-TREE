/* =========================================================
 * main.c — 진입점 및 SQL 실행 루프
 *
 * 담당자 : 김원우 (역할 D)
 * 금지   : 다른 팀원은 이 파일을 수정하지 않는다.
 *
 * 변경 이력:
 *   - index_init() 호출 추가 (run_statement 내)
 *   - index_cleanup() 호출 추가 (main 종료 전)
 *   - #include index_manager.h 추가
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/interface.h"
#include "../include/index_manager.h"

/* =========================================================
 * print_pretty_table
 * ========================================================= */
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
    } while(0)

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

/* =========================================================
 * split_tokens — ';' 기준으로 TokenList 를 분할
 * ========================================================= */
static TokenList *split_tokens(const TokenList *all, int start,
                                int *next_start) {
    int end = start;
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

    sub->count  = token_count + 1;
    sub->tokens = (Token *)calloc((size_t)sub->count, sizeof(Token));
    if (!sub->tokens) { free(sub); return NULL; }

    for (int i = 0; i < token_count; i++)
        sub->tokens[i] = all->tokens[start + i];

    sub->tokens[token_count].type  = TOKEN_EOF;
    sub->tokens[token_count].value[0] = '\0';
    sub->tokens[token_count].line  =
        all->tokens[end > 0 ? end - 1 : 0].line;

    *next_start = (end < all->count &&
                   all->tokens[end].type == TOKEN_SEMICOLON)
                  ? end + 1 : end;
    return sub;
}

/* =========================================================
 * run_statement — 파싱 → 스키마 → 인덱스 초기화 → 실행
 * ========================================================= */
static int run_statement(TokenList *tokens) {
    /* 1. 파싱 */
    ASTNode *ast = parser_parse(tokens);
    if (!ast) {
        fprintf(stderr, "Error: parsing failed\n");
        return SQL_ERR;
    }

    /* 2. 스키마 로딩 */
    const char *table = (ast->type == STMT_SELECT)
                        ? ast->select.table
                        : ast->insert.table;

    TableSchema *schema = schema_load(table);
    if (!schema) {
        fprintf(stderr, "Error: schema not found for table '%s'\n", table);
        parser_free(ast);
        return SQL_ERR;
    }

    /* 3. 스키마 검증 */
    if (schema_validate(ast, schema) != SQL_OK) {
        fprintf(stderr, "Error: schema validation failed\n");
        schema_free(schema);
        parser_free(ast);
        return SQL_ERR;
    }

    /* 4. 인덱스 초기화 (이미 초기화된 경우 즉시 반환) */
    index_init(table, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);

    /* 5. 실행 */
    if (ast->type == STMT_SELECT) {
        ResultSet *rs = db_select(&ast->select, schema);
        print_pretty_table(rs);
        result_free(rs);
    } else {
        if (db_insert(&ast->insert, schema) != SQL_OK) {
            fprintf(stderr, "Error: insert failed\n");
            schema_free(schema);
            parser_free(ast);
            return SQL_ERR;
        }
        printf("1 row inserted.\n");
    }

    schema_free(schema);
    parser_free(ast);
    return SQL_OK;
}

/* =========================================================
 * main
 *
 * 사용법: ./sqlp <sql_file>
 * ========================================================= */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <sql_file>\n", argv[0]);
        return 1;
    }

    char *sql = input_read_file(argv[1]);
    if (!sql) {
        fprintf(stderr, "Error: cannot open '%s'\n", argv[1]);
        return 1;
    }

    TokenList *all_tokens = lexer_tokenize(sql);
    free(sql);
    if (!all_tokens) {
        fprintf(stderr, "Error: tokenization failed\n");
        return 1;
    }

    int total = 0;
    int fail  = 0;
    int pos   = 0;

    while (pos < all_tokens->count) {
        if (all_tokens->tokens[pos].type == TOKEN_EOF) break;

        int next = 0;
        TokenList *sub = split_tokens(all_tokens, pos, &next);
        if (sub) {
            total++;
            if (run_statement(sub) != SQL_OK) fail++;
            lexer_free(sub);
        }
        pos = next;
    }

    lexer_free(all_tokens);

    if (total > 1) {
        printf("\n%d statement(s) executed", total);
        if (fail > 0) printf(", %d failed", fail);
        printf(".\n");
    }

    /* 인덱스 메모리 해제 */
    index_cleanup();

    return fail > 0 ? 1 : 0;
}
