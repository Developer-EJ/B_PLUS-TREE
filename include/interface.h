#ifndef INTERFACE_H
#define INTERFACE_H

/* =========================================================
 * interface.h — 모듈 경계 계약
 *
 * 팀원:
 *   역할 A (B+ Tree 알고리즘)  : 김용   → include/bptree.h, src/bptree/bptree.c
 *   역할 B (인덱스 매니저)     : 김은재 → include/index_manager.h, src/index/index_manager.c
 *   역할 C (SQL 파서 확장)     : 김규민 → src/input/lexer.c, src/parser/parser.c, src/schema/schema.c
 *   역할 D (Executor + 성능)   : 김원우 → src/executor/executor.c, src/main.c
 *
 * 규칙:
 *   1. 선언(declaration)만 허용. 구현(definition) 금지.
 *   2. 수정 시 4명 전원 합의 필수.
 *   3. 메모리 소유권은 주석으로 명시한다.
 * ========================================================= */

/* =========================================================
 * 공통 에러 코드
 * ========================================================= */
#define SQL_OK   0
#define SQL_ERR -1

/* =========================================================
 * 모듈 C (김규민) → 모듈 D (김원우) : 토큰 목록
 * ========================================================= */
typedef enum {
    TOKEN_SELECT,
    TOKEN_INSERT,
    TOKEN_INTO,
    TOKEN_FROM,
    TOKEN_WHERE,
    TOKEN_VALUES,
    TOKEN_BETWEEN,  /* BETWEEN 키워드 — 김규민 추가 */
    TOKEN_AND,      /* AND 키워드 (BETWEEN ~ AND ~ 에서 사용) — 김규민 추가 */
    TOKEN_STAR,     /* * */
    TOKEN_COMMA,    /* , */
    TOKEN_LPAREN,   /* ( */
    TOKEN_RPAREN,   /* ) */
    TOKEN_EQ,       /* = */
    TOKEN_SEMICOLON,/* ; */
    TOKEN_IDENT,    /* 테이블명, 컬럼명 */
    TOKEN_STRING,   /* 'alice' */
    TOKEN_INTEGER,  /* 42 */
    TOKEN_EOF
} TokenType;

typedef struct {
    TokenType type;
    char      value[256];  /* 토큰 원문 */
    int       line;        /* 에러 리포팅용 줄 번호 */
} Token;

typedef struct {
    Token *tokens;
    int    count;
} TokenList;

/* 모듈 C 구현 (김규민) — 호출자가 free 책임 */
char      *input_read_file(const char *path);   /* 반환값: 호출자가 free() */
TokenList *lexer_tokenize(const char *sql);      /* 반환값: 호출자가 lexer_free() */
void       lexer_free(TokenList *list);

/* =========================================================
 * 모듈 C (김규민) → 모듈 D (김원우) : AST
 * ========================================================= */
typedef enum {
    STMT_SELECT,
    STMT_INSERT
} StmtType;

/* WHERE 절 조건 타입 — 김규민 추가 */
typedef enum {
    WHERE_EQ,      /* col = val                  */
    WHERE_BETWEEN  /* col BETWEEN val_from AND val_to */
} WhereType;

typedef struct {
    char      col[64];        /* WHERE 컬럼명 */
    WhereType type;           /* 조건 종류 */
    char      val[256];       /* WHERE_EQ: 비교값 */
    char      val_from[256];  /* WHERE_BETWEEN: 시작값 */
    char      val_to[256];    /* WHERE_BETWEEN: 끝값   */
} WhereClause;

typedef struct {
    int          select_all;    /* SELECT * 이면 1 */
    char       **columns;       /* SELECT col1, col2 → ["col1","col2"] */
    int          column_count;
    char         table[64];
    int          has_where;
    WhereClause  where;
} SelectStmt;

typedef struct {
    char   table[64];
    char **columns;      /* 지정된 컬럼명 목록. NULL = 미지정 (기존 방식) */
    int    column_count; /* 지정된 컬럼 수.   0   = 미지정 (기존 방식) */
    char **values;       /* VALUES 안의 값 목록 */
    int    value_count;
} InsertStmt;

typedef struct {
    StmtType type;
    union {
        SelectStmt select;
        InsertStmt insert;
    };
} ASTNode;

/* 모듈 C 구현 (김규민) — 호출자가 free 책임 */
ASTNode *parser_parse(TokenList *tokens);  /* 반환값: 호출자가 parser_free() */
void     parser_free(ASTNode *node);

/* =========================================================
 * 모듈 C (김규민) → 모듈 D (김원우) : 스키마
 * ========================================================= */
typedef enum {
    COL_INT,
    COL_VARCHAR,
    COL_BOOLEAN  /* 허용값: "T" / "F" */
} ColType;

typedef struct {
    char    name[64];
    ColType type;
    int     max_len;    /* VARCHAR 전용, INT 는 0 */
} ColDef;

typedef struct {
    char    table_name[64];
    ColDef *columns;
    int     column_count;
} TableSchema;

/* 모듈 C 구현 (김규민) — 호출자가 free 책임 */
TableSchema *schema_load(const char *table_name);
int          schema_validate(const ASTNode *node,
                             const TableSchema *schema);
void         schema_free(TableSchema *schema);

/* =========================================================
 * 모듈 D (김원우) : 실행 결과
 * ========================================================= */
typedef struct {
    char **values;
    int    count;
} Row;

typedef struct {
    char **col_names;
    int    col_count;
    Row   *rows;
    int    row_count;
} ResultSet;

/* 모듈 D 구현 (김원우) — 호출자가 free 책임 */
int        executor_run(const ASTNode *node, const TableSchema *schema);
ResultSet *db_select(const SelectStmt *stmt, const TableSchema *schema);
int        db_insert(const InsertStmt *stmt, const TableSchema *schema);
void       result_free(ResultSet *rs);

#endif /* INTERFACE_H */
