# AGENT.md — AI 에이전트 필독 진입점

> **이 파일은 AI 에이전트가 이 저장소에서 작업하기 전에 반드시 읽어야 하는 단일 진입점이다.**
> 파일을 읽지 않고 코드를 수정하면 팀의 분업 계약을 위반하고 빌드를 깨뜨릴 수 있다.

---

## 1. 프로젝트 개요

간단한 C 언어 SQL 처리기(SELECT, INSERT)에 **B+ 트리 인덱스**를 추가해 대용량 데이터에서의 성능을 비교하는 프로젝트.

- **언어**: C (C99 표준, `-std=c99 -Wall -Wextra`)
- **플랫폼**: Linux/macOS + Windows 모두 지원
- **빌드**: Makefile (`make`, `make sim`, `make perf`, `make test`)

---

## 2. 팀 구성 및 파일 소유권 — 절대 규칙

> **이 규칙은 협업 계약이다. AI 에이전트는 반드시 아래 소유권을 존중해야 한다.**

| 역할 | 담당자 | 소유 파일 (수정 가능) |
|------|--------|----------------------|
| A — B+ Tree 알고리즘 | 김용 | `include/bptree.h`, `src/bptree/bptree.c` |
| B — 인덱스 매니저 | 김은재 | `include/index_manager.h`, `src/index/index_manager.c` |
| C — SQL 파서 확장 | 김규민 | `src/input/lexer.c`, `src/parser/parser.c`, `src/schema/schema.c` |
| D — Executor + 성능 | 김원우 | `src/executor/executor.c`, `src/main.c` |
| 공통 | 전원 합의 | `include/interface.h`, `Makefile` |

### 2-1. 파일 수정 전 확인 절차

작업 요청이 들어오면 **반드시 이 순서대로 확인**한다:

1. 수정 대상 파일이 누구 소유인지 위 표에서 확인한다.
2. 요청자가 해당 파일의 소유자인지 확인한다.
3. `include/interface.h` 또는 `Makefile` 수정은 반드시 팀 전원 합의가 전제됨을 명시한다.
4. 소유자가 아닌 파일은 절대로 수정하지 않는다.

---

## 3. 아키텍처

```
SQL 파일
   │
   ▼
[Lexer]          src/input/lexer.c          — C: 김규민
   │              TOKEN_BETWEEN / TOKEN_AND 추가
   ▼
[Parser]         src/parser/parser.c        — C: 김규민
   │              WHERE col BETWEEN a AND b 지원
   ▼
[Schema]         src/schema/schema.c        — C: 김규민
   │              BETWEEN 컬럼 타입 검증
   │
   ├──[INSERT]──▶ executor.c::db_insert     — D: 김원우
   │                  fopen("ab") → ftell() → fprintf() → index_insert_*()
   │                                              │
   │                                     [Index Manager]  — B: 김은재
   │                                     src/index/index_manager.c
   │                                              │
   │                                    ┌─────────┴────────┐
   │                              [B+ Tree #1]       [B+ Tree #2]
   │                              key=id             key=(id,age)
   │                              A: 김용             A: 김용
   │
   └──[SELECT]──▶ executor.c::db_select    — D: 김원우
                      WHERE id=?              → index_search_id() + fetch_by_offset()
                      WHERE id BETWEEN a AND b → index_range_id() + fetch_by_offsets()
                      그 외 (name=? 등)       → linear_scan()
                      (결과: stderr에 타이밍 출력)
```

---

## 4. 핵심 인터페이스 계약

### 4-1. interface.h (공유 — 수정 금지)

```c
// 토큰 타입 (추가된 항목)
TOKEN_BETWEEN, TOKEN_AND,

// WHERE 절 타입
typedef enum { WHERE_EQ, WHERE_BETWEEN } WhereType;

typedef struct {
    char      col[64];
    WhereType type;
    char      val[256];       /* WHERE_EQ 전용 */
    char      val_from[256];  /* WHERE_BETWEEN 전용 */
    char      val_to[256];    /* WHERE_BETWEEN 전용 */
} WhereClause;
```

### 4-2. bptree.h (A: 김용 소유)

```c
// 단일 키 트리 (key=id, value=file_offset)
BPTree *bptree_create(int order);
void    bptree_destroy(BPTree *tree);
int     bptree_insert(BPTree *tree, int key, long value);
long    bptree_search(BPTree *tree, int key);              // 없으면 -1
int     bptree_range(BPTree *tree, int from, int to,
                     long *out, int max_count);
int     bptree_height(BPTree *tree);
void    bptree_print(BPTree *tree);

// 복합 키 트리 (key1=id, key2=age)
BPTreeComp *bptree_comp_create(int order);
void        bptree_comp_destroy(BPTreeComp *tree);
int         bptree_comp_insert(BPTreeComp *tree, int key1, int key2, long value);
long        bptree_comp_search(BPTreeComp *tree, int key1, int key2); // 없으면 -1
int         bptree_comp_height(BPTreeComp *tree);
```

### 4-3. index_manager.h (B: 김은재 소유)

```c
#define IDX_ORDER_DEFAULT  128   /* 낮은 트리 (운영) */
#define IDX_ORDER_SMALL      4   /* 높은 트리 (실험) */
#define IDX_MAX_RANGE    65536
#define IDX_MAX_TABLES       8

int  index_init(const char *table, int order_id, int order_comp); /* 멱등 */
void index_cleanup(void);

int  index_insert_id(const char *table, int id, long offset);
long index_search_id(const char *table, int id);          /* 없으면 -1 */
int  index_range_id(const char *table, int from, int to,
                    long *offsets, int max);

int  index_insert_comp(const char *table, int id, int age, long offset);
long index_search_comp(const char *table, int id, int age); /* 없으면 -1 */

int  index_height_id(const char *table);
int  index_height_comp(const char *table);
```

---

## 5. 구현 상태 (현재 기준)

| 파일 | 상태 | 비고 |
|------|------|------|
| `include/interface.h` | ✅ 완료 | WhereClause, Token 확장 포함 |
| `include/bptree.h` | ✅ 완료 | API 선언 완료 |
| `include/index_manager.h` | ✅ 완료 | API 선언 완료 |
| `src/bptree/bptree.c` | ⚠️ 스텁 | 알고리즘 미구현, 컴파일만 가능 |
| `src/index/index_manager.c` | ⚠️ 스텁 | 파일 스캔 미구현, 컴파일만 가능 |
| `src/input/lexer.c` | ✅ 완료 | BETWEEN/AND 키워드 추가 |
| `src/parser/parser.c` | ✅ 완료 | BETWEEN 파싱 분기 완료 |
| `src/schema/schema.c` | ✅ 완료 | BETWEEN 타입 검증 완료 |
| `src/executor/executor.c` | ✅ 완료 | 분기 로직, 타이밍 출력 완료 |
| `src/main.c` | ✅ 완료 | index_init/cleanup 호출 포함 |
| `tests/test_perf.c` | ⚠️ 스텁 | bench_* 함수 TODO 마커 있음 |

---

## 6. 필수 코딩 규칙

### 6-1. 파일 I/O — Windows 오프셋 일치 필수

```c
/* INSERT 쓰기 — binary append 모드만 사용 */
FILE *fp = fopen(path, "ab");    /* ← "ab" 필수. "a"(text)는 사용 금지 */
long offset = ftell(fp);         /* 행 시작 오프셋 기록 */
/* ... 데이터 쓰기 ... */
fclose(fp);

/* SELECT 읽기 — binary 읽기 모드만 사용 */
FILE *fp = fopen(path, "rb");    /* ← "rb" 필수 */
fseek(fp, offset, SEEK_SET);
fgets(line, sizeof(line), fp);
```

> 이유: Windows text mode는 `\r\n` ↔ `\n` 변환으로 ftell 오프셋이 어긋난다.

### 6-2. 시간 측정

```c
static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}
/* clock()은 C99 표준. clock_gettime()은 POSIX 전용이므로 사용 금지. */
```

### 6-3. 성능 출력 포맷 (stderr 전용)

```
[SELECT][index:id:eq         ]    0.012 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][index:id:range      ]    0.087 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][linear              ]  342.110 ms  tree_h(id)=4  tree_h(comp)=4
```

- `stdout`: 결과 테이블 행만 출력
- `stderr`: 타이밍/진단 출력 전용

### 6-4. 디스크 I/O 시뮬레이션

```c
/* bptree.c 내부 — 레벨 이동마다 호출 */
#if BPTREE_SIMULATE_IO
  #ifdef _WIN32
    #include <windows.h>
    #define IO_SLEEP() Sleep(DISK_IO_DELAY_US / 1000)
  #else
    #include <unistd.h>
    #define IO_SLEEP() usleep(DISK_IO_DELAY_US)
  #endif
#else
  #define IO_SLEEP() ((void)0)
#endif
```

`make sim` 으로 빌드 시 `-DBPTREE_SIMULATE_IO=1` 이 활성화된다.

### 6-5. 스키마 (schema/users.schema)

```
col0=id,INT,0         ← B+ Tree #1 인덱스 키, B+ Tree #2 key1
col1=name,VARCHAR,64
col2=age,INT,0        ← B+ Tree #2 key2
col3=email,VARCHAR,128
```

### 6-6. 데이터 파일 경로 규칙

```
data/{table}.dat       ← INSERT 결과 저장 (binary append)
schema/{table}.schema  ← 컬럼 정의
samples/*.sql          ← 입력 SQL 파일
```

---

## 7. 자동화 훅 및 검증 스크립트

### 7-1. 초기 설치 (클론 후 한 번만 실행)

```bash
bash scripts/install_hooks.sh
```

`.githooks/pre-commit` 을 `.git/hooks/pre-commit` 에 복사한다.
이후 `git commit` 실행 시 자동으로 세 가지 검증이 돌아간다.

### 7-2. 프리커밋 훅 — 검증 순서

```
git commit 실행
   │
   ├─ [1/3] 린터 (scripts/lint.sh)
   │         gcc -std=c99 -Wall -Wextra -fsyntax-only 로 전체 소스 컴파일 검증
   │         실패 시 → 개별 파일 진단 후 에러 출력
   │
   ├─ [2/3] 소유권 가드 (scripts/check_ownership.sh)
   │         스테이징된 파일이 한 역할의 소유 파일만 포함하는지 확인
   │         여러 역할 혼재 → 커밋 차단
   │         전원 합의 필요 파일(interface.h, Makefile) → 경고 출력
   │
   └─ [3/3] binary 모드 (scripts/check_binary_mode.sh)
             executor.c, index_manager.c 의 fopen() 이
             text mode ("r","a","w") 를 사용하는지 검사
             발견 시 → 커밋 차단

결과:
   PASS → 커밋 허용, logs/pre_commit_failures.log 에 타임스탬프 기록
   FAIL → 커밋 차단, logs/pre_commit_failures.log 에 실패 상세 기록
```

### 7-3. 피드백 루프 — 실패 시 대응 절차

```
1. git commit 차단됨
2. logs/pre_commit_failures.log 에 실패 내용 기록됨
3. AI 에이전트 또는 개발자가 로그를 읽고 문제 수정
4. 수정 후 다시 git add + git commit 시도
5. 모든 검증 통과 → 커밋 성공
```

`logs/pre_commit_failures.log` — 시간순으로 누적됨. 정기적으로 확인 권장.

### 7-4. 수동 실행

```bash
bash scripts/lint.sh               # 컴파일 에러만 확인
bash scripts/check_ownership.sh    # 소유권만 확인
bash scripts/check_binary_mode.sh  # binary fopen 만 확인
bash .git/hooks/pre-commit         # 전체 파이프라인 수동 실행
```

---

## 9. 빌드 명령

```bash
make          # 기본 빌드 → ./sqlp
make sim      # I/O 시뮬레이션 → ./sqlp_sim  (-DBPTREE_SIMULATE_IO=1)
make perf     # 성능 비교 실행 파일 → ./test_perf
make perf_sim # 성능 비교 + I/O 시뮬레이션 → ./test_perf_sim
make gen_data # 데이터 생성기 → ./gen_data
make test     # 단위 테스트 실행
make clean    # 빌드 결과물 삭제
```

---

## 10. 지원 SQL 문법

```sql
-- INSERT
INSERT INTO users VALUES (1, 'alice', 25, 'alice@example.com');
INSERT INTO users (id, name, age, email) VALUES (2, 'bob', 30, 'bob@example.com');

-- SELECT
SELECT * FROM users;
SELECT * FROM users WHERE id = 42;
SELECT * FROM users WHERE name = 'alice';
SELECT * FROM users WHERE id BETWEEN 100 AND 200;
```

---

## 11. 미완료 작업 (TODO)

1. **A (김용)**: `src/bptree/bptree.c` — BPNode 구조체 및 B+ 트리 알고리즘 실제 구현
   - 노드 분열(split), 리프 링크드 리스트, range 쿼리, IO_SLEEP 적용

2. **B (김은재)**: `src/index/index_manager.c` — `index_init()` 내 `.dat` 파일 스캔 구현
   - `"rb"` 모드로 열고 `ftell` → `fgets` → id/age 파싱 → 두 트리에 삽입

3. **공통**: 단위 테스트 파일 작성
   - `tests/test_bptree.c`, `tests/test_index.c`
   - `tests/test_parser.c`, `tests/test_schema.c`, `tests/test_executor.c`

4. **공통**: `tests/test_perf.c` 의 TODO 마커 완성 (선형 스캔 측정 코드)

---

## 12. AI 에이전트 행동 규칙 요약

1. **이 파일을 먼저 읽는다** — 작업 전 반드시 완독한다.
2. **파일 소유권을 확인한다** — 섹션 2의 표를 보고 수정 대상 파일의 소유자를 확인한다.
3. **interface.h와 Makefile은 혼자 수정하지 않는다** — 팀 합의 필요 파일이다.
4. **binary 파일 모드를 유지한다** — `"ab"`, `"rb"` 규칙(섹션 6-1)을 절대 바꾸지 않는다.
5. **stderr/stdout 출력 규칙을 지킨다** — 타이밍은 stderr, 결과 행은 stdout.
6. **스텁을 삭제하지 않는다** — 아직 구현 전인 함수의 스텁은 유지한다.
7. **스키마를 임의로 바꾸지 않는다** — `schema/users.schema` 는 팀 계약이다.
8. **clock() 을 사용한다** — `clock_gettime()`, `gettimeofday()` 등 비표준 함수 금지.
