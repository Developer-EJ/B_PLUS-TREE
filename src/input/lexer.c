/* =========================================================
 * lexer.c — SQL 토크나이저
 *
 * 담당자 : 김규민 (역할 C)
 * 금지   : 다른 팀원은 이 파일을 수정하지 않는다.
 *
 * 변경 이력:
 *   - BETWEEN, AND 키워드 추가 (TOKEN_BETWEEN, TOKEN_AND)
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "../../include/interface.h"

/* ── 키워드 테이블 ──────────────────────────────────────── */
static struct { const char *word; TokenType type; } keywords[] = {
    {"SELECT",  TOKEN_SELECT},
    {"INSERT",  TOKEN_INSERT},
    {"INTO",    TOKEN_INTO},
    {"FROM",    TOKEN_FROM},
    {"WHERE",   TOKEN_WHERE},
    {"VALUES",  TOKEN_VALUES},
    {"BETWEEN", TOKEN_BETWEEN},  /* 김규민 추가 */
    {"AND",     TOKEN_AND},      /* 김규민 추가 — BETWEEN ~ AND ~ 에서 사용 */
    {NULL,      TOKEN_EOF}
};

static TokenType keyword_lookup(const char *word) {
    char upper[64];
    int i;
    for (i = 0; word[i] && i < 63; i++)
        upper[i] = (char)toupper((unsigned char)word[i]);
    upper[i] = '\0';

    for (int k = 0; keywords[k].word; k++) {
        if (strcmp(upper, keywords[k].word) == 0)
            return keywords[k].type;
    }
    return TOKEN_IDENT;
}

static int append_token(TokenList *list, TokenType type,
                        const char *value, int line) {
    if (!list) return SQL_ERR;

    if (list->count == 0) {
        list->tokens = (Token *)calloc(8, sizeof(Token));
    } else if (list->count % 8 == 0) {
        size_t new_cap = (size_t)list->count * 2;
        Token *next = (Token *)realloc(list->tokens,
                                        new_cap * sizeof(Token));
        if (!next) return SQL_ERR;
        list->tokens = next;
    }

    if (!list->tokens) return SQL_ERR;

    Token *tok = &list->tokens[list->count++];
    tok->type = type;
    tok->line = line;
    if (value) {
        strncpy(tok->value, value, sizeof(tok->value) - 1);
        tok->value[sizeof(tok->value) - 1] = '\0';
    } else {
        tok->value[0] = '\0';
    }
    return SQL_OK;
}

TokenList *lexer_tokenize(const char *sql) {
    if (!sql) return NULL;

    TokenList *list = (TokenList *)malloc(sizeof(TokenList));
    if (!list) return NULL;
    list->tokens = NULL;
    list->count  = 0;

    int         line = 1;
    const char *p    = sql;

    while (*p) {
        /* 공백 / 줄바꿈 건너뛰기 */
        while (isspace((unsigned char)*p)) {
            if (*p == '\n') line++;
            p++;
        }
        if (!*p) break;

        /* 식별자 / 키워드 */
        if (isalpha((unsigned char)*p) || *p == '_') {
            const char *start = p;
            while (isalnum((unsigned char)*p) || *p == '_') p++;
            size_t len = (size_t)(p - start);

            char word[64];
            if (len >= sizeof(word)) len = sizeof(word) - 1;
            memcpy(word, start, len);
            word[len] = '\0';

            TokenType type = keyword_lookup(word);
            if (append_token(list, type, word, line) == SQL_ERR)
                goto fail;
            continue;
        }

        /* 정수 리터럴 */
        if (isdigit((unsigned char)*p)) {
            const char *start = p;
            while (isdigit((unsigned char)*p)) p++;
            size_t len = (size_t)(p - start);

            char num[256];
            if (len >= sizeof(num)) len = sizeof(num) - 1;
            memcpy(num, start, len);
            num[len] = '\0';

            if (append_token(list, TOKEN_INTEGER, num, line) == SQL_ERR)
                goto fail;
            continue;
        }

        /* 문자열 리터럴 '...' */
        if (*p == '\'') {
            p++;
            const char *start = p;
            while (*p && *p != '\'') p++;
            if (!*p) {
                fprintf(stderr,
                        "lexer: unterminated string literal at line %d\n",
                        line);
                goto fail;
            }
            size_t len = (size_t)(p - start);
            char value[256];
            if (len >= sizeof(value)) len = sizeof(value) - 1;
            memcpy(value, start, len);
            value[len] = '\0';

            if (append_token(list, TOKEN_STRING, value, line) == SQL_ERR)
                goto fail;
            p++;
            continue;
        }

        /* 단일 문자 기호 */
        if (*p == '*') { if (append_token(list, TOKEN_STAR,      "*", line) == SQL_ERR) goto fail; p++; continue; }
        if (*p == ',') { if (append_token(list, TOKEN_COMMA,     ",", line) == SQL_ERR) goto fail; p++; continue; }
        if (*p == '(') { if (append_token(list, TOKEN_LPAREN,    "(", line) == SQL_ERR) goto fail; p++; continue; }
        if (*p == ')') { if (append_token(list, TOKEN_RPAREN,    ")", line) == SQL_ERR) goto fail; p++; continue; }
        if (*p == '=') { if (append_token(list, TOKEN_EQ,        "=", line) == SQL_ERR) goto fail; p++; continue; }
        if (*p == ';') { if (append_token(list, TOKEN_SEMICOLON, ";", line) == SQL_ERR) goto fail; p++; continue; }

        fprintf(stderr,
                "lexer: unknown character '%c' at line %d\n", *p, line);
        goto fail;
    }

    if (append_token(list, TOKEN_EOF, "", line) == SQL_ERR) goto fail;
    return list;

fail:
    lexer_free(list);
    return NULL;
}

void lexer_free(TokenList *list) {
    if (!list) return;
    free(list->tokens);
    free(list);
}
