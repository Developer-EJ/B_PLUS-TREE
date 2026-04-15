#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/interface.h"

static int failures = 0;
static int tests_run = 0;

#define EXPECT_TRUE(cond, msg) do {                                      \
    if (!(cond)) {                                                       \
        fprintf(stderr, "[FAIL] %s:%d: %s\n", __FILE__, __LINE__, msg);  \
        failures++;                                                      \
        goto cleanup;                                                    \
    }                                                                    \
} while (0)

static ASTNode *parse_sql(const char *sql) {
    TokenList *tokens = lexer_tokenize(sql);
    if (!tokens) return NULL;

    ASTNode *node = parser_parse(tokens);
    lexer_free(tokens);
    return node;
}

static void run_test(const char *name, void (*fn)(void)) {
    int before = failures;
    fn();
    tests_run++;
    if (failures == before) printf("[PASS] %s\n", name);
}

static void test_lexer_recognizes_between_and(void) {
    TokenList *tokens = NULL;
    TokenType expected[] = {
        TOKEN_SELECT,
        TOKEN_STAR,
        TOKEN_FROM,
        TOKEN_IDENT,
        TOKEN_WHERE,
        TOKEN_IDENT,
        TOKEN_BETWEEN,
        TOKEN_INTEGER,
        TOKEN_AND,
        TOKEN_INTEGER,
        TOKEN_EOF
    };

    tokens = lexer_tokenize("SELECT * FROM users WHERE id BETWEEN 1 AND 100");
    EXPECT_TRUE(tokens != NULL, "lexer_tokenize should succeed");
    EXPECT_TRUE(tokens->count == (int)(sizeof(expected) / sizeof(expected[0])),
                "unexpected token count for BETWEEN query");

    for (int i = 0; i < tokens->count; i++) {
        EXPECT_TRUE(tokens->tokens[i].type == expected[i],
                    "unexpected token type sequence");
    }

cleanup:
    lexer_free(tokens);
}

static void test_parse_where_between(void) {
    ASTNode *node = NULL;

    node = parse_sql("SELECT * FROM users WHERE id BETWEEN 1 AND 100");
    EXPECT_TRUE(node != NULL, "parser should return an AST for valid BETWEEN");
    EXPECT_TRUE(node->type == STMT_SELECT, "statement should be SELECT");
    EXPECT_TRUE(node->select.select_all == 1, "SELECT * should set select_all");
    EXPECT_TRUE(strcmp(node->select.table, "users") == 0,
                "table name should be users");
    EXPECT_TRUE(node->select.has_where == 1, "BETWEEN query should have WHERE");
    EXPECT_TRUE(strcmp(node->select.where.col, "id") == 0,
                "WHERE column should be id");
    EXPECT_TRUE(node->select.where.type == WHERE_BETWEEN,
                "WHERE type should be WHERE_BETWEEN");
    EXPECT_TRUE(strcmp(node->select.where.val_from, "1") == 0,
                "BETWEEN lower bound should be 1");
    EXPECT_TRUE(strcmp(node->select.where.val_to, "100") == 0,
                "BETWEEN upper bound should be 100");
    EXPECT_TRUE(node->select.where.val[0] == '\0',
                "WHERE_EQ value should remain empty for BETWEEN");

cleanup:
    parser_free(node);
}

static void test_parse_where_eq(void) {
    ASTNode *node = NULL;

    node = parse_sql("SELECT * FROM users WHERE id = 42");
    EXPECT_TRUE(node != NULL, "parser should return an AST for valid equality");
    EXPECT_TRUE(node->type == STMT_SELECT, "statement should be SELECT");
    EXPECT_TRUE(node->select.has_where == 1, "equality query should have WHERE");
    EXPECT_TRUE(strcmp(node->select.where.col, "id") == 0,
                "WHERE column should be id");
    EXPECT_TRUE(node->select.where.type == WHERE_EQ,
                "WHERE type should be WHERE_EQ");
    EXPECT_TRUE(strcmp(node->select.where.val, "42") == 0,
                "WHERE_EQ value should be 42");
    EXPECT_TRUE(node->select.where.val_from[0] == '\0',
                "BETWEEN lower bound should remain empty for equality");
    EXPECT_TRUE(node->select.where.val_to[0] == '\0',
                "BETWEEN upper bound should remain empty for equality");

cleanup:
    parser_free(node);
}

static void test_parse_between_with_column_list(void) {
    ASTNode *node = NULL;

    node = parse_sql("SELECT id, name FROM users WHERE id BETWEEN 1 AND 50");
    EXPECT_TRUE(node != NULL, "parser should support column lists with BETWEEN");
    EXPECT_TRUE(node->type == STMT_SELECT, "statement should be SELECT");
    EXPECT_TRUE(node->select.select_all == 0,
                "column-list SELECT should not set select_all");
    EXPECT_TRUE(node->select.column_count == 2,
                "column-list query should have two selected columns");
    EXPECT_TRUE(strcmp(node->select.columns[0], "id") == 0,
                "first selected column should be id");
    EXPECT_TRUE(strcmp(node->select.columns[1], "name") == 0,
                "second selected column should be name");
    EXPECT_TRUE(node->select.where.type == WHERE_BETWEEN,
                "WHERE type should be WHERE_BETWEEN");
    EXPECT_TRUE(strcmp(node->select.where.val_from, "1") == 0,
                "BETWEEN lower bound should be 1");
    EXPECT_TRUE(strcmp(node->select.where.val_to, "50") == 0,
                "BETWEEN upper bound should be 50");

cleanup:
    parser_free(node);
}

static void test_parse_incomplete_between_fails(void) {
    ASTNode *node = parse_sql("SELECT * FROM users WHERE id BETWEEN 100 AND");
    EXPECT_TRUE(node == NULL, "incomplete BETWEEN should fail to parse");

cleanup:
    parser_free(node);
}

int main(void) {
    run_test("lexer recognizes BETWEEN and AND", test_lexer_recognizes_between_and);
    run_test("parser handles WHERE BETWEEN", test_parse_where_between);
    run_test("parser handles WHERE equality", test_parse_where_eq);
    run_test("parser handles column list with BETWEEN", test_parse_between_with_column_list);
    run_test("parser rejects incomplete BETWEEN", test_parse_incomplete_between_fails);

    if (failures > 0) {
        fprintf(stderr, "\n%d/%d parser tests failed.\n", failures, tests_run);
        return 1;
    }

    printf("\nAll %d parser tests passed.\n", tests_run);
    return 0;
}
