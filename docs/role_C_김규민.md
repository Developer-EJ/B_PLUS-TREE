# 역할 C — SQL 파서 확장 (BETWEEN)

**담당자**: 김규민  
**시작 가능**: Day 1 즉시 (B+ Tree / 인덱스와 완전 독립)

---

## 소유 파일 (이 파일만 수정 가능)

| 파일 | 설명 |
|------|------|
| `src/input/lexer.c` | 토크나이저 — `TOKEN_BETWEEN`, `TOKEN_AND` 추가 |
| `src/parser/parser.c` | 파서 — `WHERE col BETWEEN a AND b` 파싱 |
| `src/schema/schema.c` | 스키마 검증 — BETWEEN 대상 컬럼 타입 검증 |

## 절대 수정 금지 파일

```
include/interface.h       ← 전원 합의 없이 수정 금지 (이미 변경 완료)
include/bptree.h          ← 김용 소유
include/index_manager.h   ← 김은재 소유
src/bptree/bptree.c       ← 김용 소유
src/index/index_manager.c ← 김은재 소유
src/executor/executor.c   ← 김원우 소유
src/main.c                ← 김원우 소유
```

---

## interface.h 변경 완료 사항 (이미 반영됨)

```c
// TokenType 에 추가됨
TOKEN_BETWEEN,
TOKEN_AND,

// WhereType 신규 추가됨
typedef enum { WHERE_EQ, WHERE_BETWEEN } WhereType;

// WhereClause 확장됨
typedef struct {
    char      col[64];
    WhereType type;
    char      val[256];       // WHERE_EQ 용
    char      val_from[256];  // WHERE_BETWEEN 용
    char      val_to[256];    // WHERE_BETWEEN 용
} WhereClause;
```

---

## 기능 명세

### lexer.c — 키워드 추가 (완료)

`BETWEEN` 과 `AND` 를 키워드 테이블에 추가한다:

```c
{"BETWEEN", TOKEN_BETWEEN},
{"AND",     TOKEN_AND},
```

### parser.c — WHERE 절 파싱 (완료)

```
WHERE col BETWEEN val_from AND val_to
  → where.type     = WHERE_BETWEEN
    where.col      = "col"
    where.val_from = "val_from"
    where.val_to   = "val_to"

WHERE col = val
  → where.type = WHERE_EQ
    where.col  = "col"
    where.val  = "val"
```

### schema.c — BETWEEN 검증 (완료)

WHERE_BETWEEN 조건일 때 추가 검증:

1. 대상 컬럼이 `COL_INT` 타입인지 확인
2. `val_from` 과 `val_to` 가 정수 문자열인지 확인

---

## 지원 SQL 문법 (구현 완료 후 기준)

```sql
-- 기존 (변경 없음)
SELECT * FROM users;
SELECT id, name FROM users WHERE id = 42;
INSERT INTO users VALUES (1, 'alice', 25, 'alice@example.com');

-- 신규 (역할 C 추가)
SELECT * FROM users WHERE id BETWEEN 100 AND 200;
SELECT id, name FROM users WHERE id BETWEEN 1 AND 50;
```

---

## 단위 테스트 기준 (`tests/test_parser.c`)

| SQL | 기대 결과 |
|-----|----------|
| `SELECT * FROM users WHERE id BETWEEN 1 AND 100` | `has_where=1`, `type=WHERE_BETWEEN`, `val_from="1"`, `val_to="100"` |
| `SELECT * FROM users WHERE id = 42` | `has_where=1`, `type=WHERE_EQ`, `val="42"` |
| `WHERE name BETWEEN 'a' AND 'z'` (name은 VARCHAR) | schema_validate → `SQL_ERR` (INT만 지원) |
| `WHERE id BETWEEN abc AND 100` (비정수) | schema_validate → `SQL_ERR` |
| `SELECT * FROM users WHERE id BETWEEN 100 AND` (불완전) | parser_parse → `NULL` |

---

## 개발 순서

1. ~~lexer.c — BETWEEN, AND 키워드 추가~~ (완료)
2. ~~parser.c — BETWEEN 파싱 분기 추가~~ (완료)
3. ~~schema.c — BETWEEN 검증 추가~~ (완료)
4. `tests/test_parser.c` — BETWEEN 파싱 테스트 케이스 작성
5. `tests/test_schema.c` — BETWEEN 검증 테스트 케이스 작성
6. `samples/` 에 BETWEEN 예제 SQL 파일 추가

---

## 지켜야 할 점

1. `TOKEN_AND` 는 현재 BETWEEN 문맥에서만 사용된다. 향후 `WHERE a=1 AND b=2` 와 같은 복합 조건을 지원할 때 재사용할 수 있도록 범용적으로 설계한다.
2. `WhereClause.val` 필드는 `WHERE_EQ` 에서만 사용한다. `WHERE_BETWEEN` 일 때는 `val_from` / `val_to` 를 사용한다.
3. `val` / `val_from` / `val_to` 는 `'\0'` 으로 초기화되어야 한다 (`calloc` 사용으로 자동 보장).
4. 파서가 실패하면 반드시 `parser_free(node)` 를 호출하고 `NULL` 을 반환한다 (메모리 누수 방지).
