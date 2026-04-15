# 역할 B — 인덱스 매니저

**담당자**: 김은재  
**시작 가능**: Day 1 — `bptree.h` 인터페이스 확정 즉시 (스텁 기반 병행)

---

## 소유 파일 (이 파일만 수정 가능)

| 파일 | 설명 |
|------|------|
| `include/index_manager.h` | 인덱스 매니저 공개 인터페이스 |
| `src/index/index_manager.c` | 인덱스 매니저 구현체 |

## 절대 수정 금지 파일

```
include/interface.h       ← 전원 합의 없이 수정 금지
include/bptree.h          ← 김용 소유 (읽기는 가능)
src/bptree/bptree.c       ← 김용 소유
src/input/lexer.c         ← 김규민 소유
src/parser/parser.c       ← 김규민 소유
src/schema/schema.c       ← 김규민 소유
src/executor/executor.c   ← 김원우 소유
src/main.c                ← 김원우 소유
```

---

## 기능 명세

```c
// 초기화 / 정리
int  index_init(const char *table, int order_id, int order_comp);
void index_cleanup(void);

// Tree #1: id 단일 인덱스
int  index_insert_id(const char *table, int id, long offset);
long index_search_id(const char *table, int id);    // 없으면 -1
int  index_range_id(const char *table, int from, int to,
                    long *offsets, int max);

// Tree #2: (id, age) 복합 인덱스
int  index_insert_comp(const char *table, int id, int age, long offset);
long index_search_comp(const char *table, int id, int age); // 없으면 -1

// 높이 조회 (성능 출력용)
int  index_height_id(const char *table);
int  index_height_comp(const char *table);
```

---

## 구현 요구사항

### `index_init` — 핵심 구현

기존 `.dat` 파일을 스캔해 두 트리를 동시에 구축한다:

```c
int index_init(const char *table, int order_id, int order_comp) {
    // 1. 이미 초기화된 테이블이면 즉시 0 반환 (멱등)
    // 2. bptree_create / bptree_comp_create 로 두 트리 생성
    // 3. data/{table}.dat 파일 오픈 ("rb" — binary mode 필수)
    // 4. 루프:
    //      long offset = ftell(fp);       ← 행 시작 오프셋 기록
    //      fgets(line, sizeof(line), fp); ← 한 행 읽기
    //      int id  = col_value_int(line, 0); ← 0번 컬럼 = id
    //      int age = col_value_int(line, 2); ← 2번 컬럼 = age
    //      bptree_insert(tree_id, id, offset);
    //      bptree_comp_insert(tree_comp, id, age, offset);
    // 5. g_tables[] 에 등록
}
```

**중요**: 파일을 `"rb"` (binary mode) 로 열어야 `ftell` 오프셋이 executor의 `fseek` 와 일치한다.

### 컬럼 파싱 헬퍼 (내부 static 함수)

```c
// '|' 구분자 기준 idx 번째 토큰의 정수값 반환
static int col_value_int(const char *line, int idx) {
    char buf[1024];
    strncpy(buf, line, sizeof(buf) - 1);
    char *tok = strtok(buf, "|");
    for (int i = 0; i < idx && tok; i++)
        tok = strtok(NULL, "|");
    if (!tok) return -1;
    while (*tok == ' ') tok++;
    return atoi(tok);
}
```

### 다중 테이블 관리

- `g_tables[IDX_MAX_TABLES]` 배열로 최대 8개 테이블 동시 관리
- `find_entry()` 내부 헬퍼로 테이블명 → `TableIndex*` 룩업

---

## 개발 순서

1. 스텁(`src/index/index_manager.c`)을 사용해 컴파일 확인
2. `index_init` 의 파일 스캔 로직 구현
3. `index_insert_id` / `index_search_id` 연동 확인
4. `index_range_id` 연동 확인
5. 복합 키 insert/search 확인
6. `tests/test_index.c` 단위 테스트 통과

---

## 단위 테스트 기준 (`tests/test_index.c`)

| 테스트 | 기대 결과 |
|--------|----------|
| `index_init("users", 128, 128)` | 0 (성공) |
| `index_search_id("users", 1)` | 삽입된 오프셋 |
| `index_range_id("users", 1, 100, offsets, 65536)` | 100 |
| `index_search_id("users", 999999)` — 없는 id | -1 |
| `index_search_comp("users", 1, 25)` | 삽입된 오프셋 |
| `index_height_id` order=4 vs order=128 | order=4 가 더 높음 |
| `index_cleanup()` 후 `index_search_id` | -1 (해제됨) |

---

## 지켜야 할 점

1. `index_manager.h` API 변경 시 **김원우(D)에게 즉시 공지** (executor가 직접 호출함).
2. `index_init` 은 멱등(idempotent)이어야 한다 — 같은 테이블을 두 번 호출해도 트리가 두 번 생성되지 않는다.
3. `index_cleanup` 후 모든 메모리가 해제되어야 한다.
4. `bptree_*` 함수가 스텁 상태일 때도 정상 컴파일 + 실행되어야 한다.
