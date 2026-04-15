# 역할 D — Executor + 성능 측정

**담당자**: 김원우  
**시작 가능**: Day 1 — `index_manager.h` 인터페이스 확정 즉시 (스텁 기반 병행)

---

## 소유 파일 (이 파일만 수정 가능)

| 파일 | 설명 |
|------|------|
| `src/executor/executor.c` | SQL 실행기, 인덱스 분기, 성능 출력 |
| `src/main.c` | 진입점, index_init/cleanup 호출 |

## 절대 수정 금지 파일

```
include/interface.h       ← 전원 합의 없이 수정 금지
include/bptree.h          ← 김용 소유
include/index_manager.h   ← 김은재 소유 (읽기는 가능)
src/bptree/bptree.c       ← 김용 소유
src/index/index_manager.c ← 김은재 소유
src/input/lexer.c         ← 김규민 소유
src/parser/parser.c       ← 김규민 소유
src/schema/schema.c       ← 김규민 소유
```

---

## 기능 명세

### db_select — SELECT 분기 로직

```
WHERE 조건 분석:
  ├── WHERE id = N        → index_search_id()  + fetch_by_offset()
  ├── WHERE id BETWEEN a AND b → index_range_id() + fetch_by_offsets()
  └── 그 외 (name, age 등)    → 선형 스캔 (linear_scan())
```

각 분기 실행 후 stderr 에 출력:
```
[SELECT][index:id:eq         ]    0.012 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][index:id:range      ]    0.087 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][linear              ]  342.110 ms  tree_h(id)=4  tree_h(comp)=4
```

### db_insert — INSERT + 인덱스 등록

1. `fopen(path, "ab")` — **binary append 모드** 필수 (오프셋 정확도)
2. `ftell(fp)` 로 행 시작 오프셋 기록
3. 기존 방식으로 행 저장 (`fprintf`)
4. 스키마에서 `id` / `age` 컬럼 위치를 찾아 값 파싱
5. `index_insert_id()` + `index_insert_comp()` 호출

### fetch_by_offset / fetch_by_offsets

```c
// binary mode("rb") 로 열어 fseek → fgets → Row 파싱
static ResultSet *fetch_by_offset(long offset, ...);
static ResultSet *fetch_by_offsets(const long *offsets, int count, ...);
```

### 시간 측정

```c
static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}
// 사용: double t0 = now_ms(); ... double elapsed = now_ms() - t0;
```

---

## index_init 호출 위치 (main.c)

`run_statement()` 안에서 스키마 검증 직후 호출:

```c
// 4. 인덱스 초기화 (이미 초기화된 경우 즉시 반환)
index_init(table, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);
```

`main()` 종료 직전:

```c
index_cleanup();
return fail > 0 ? 1 : 0;
```

---

## 개발 순서

1. 스텁(`index_manager.c`)을 사용해 executor 분기 로직 컴파일 확인
2. `fetch_by_offset` / `fetch_by_offsets` 구현 및 테스트
3. `db_select` 분기 로직 + 시간 출력 완성
4. `db_insert` binary mode + 인덱스 등록 완성
5. 김은재(B) 의 실제 `index_manager.c` 완성 후 스텁 제거
6. `tests/test_executor.c` 테스트 통과

---

## 단위 테스트 기준 (`tests/test_executor.c`)

| 테스트 | 기대 결과 |
|--------|----------|
| INSERT 1건 → `db_select WHERE id=1` | 1 row 반환 |
| INSERT 100건 → `db_select WHERE id BETWEEN 1 AND 50` | 50 rows 반환 |
| `db_select WHERE name='alice'` (선형 스캔) | 1 row 반환 |
| `db_select WHERE id=999` (없는 id) | 0 rows 반환 |
| stderr 출력에 `[SELECT]` 포함 | 타이밍 출력 확인 |

---

## 성능 출력 포맷 (최종 기준)

```
[SELECT][index:id:eq         ]    0.012 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][index:id:range      ]    0.087 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][linear              ]  342.110 ms  tree_h(id)=0  tree_h(comp)=0
```

- 출력 대상: `stderr` (stdout은 결과 테이블 전용)
- `tree_h` 가 0이면 인덱스 미초기화 상태

---

## 지켜야 할 점

1. `fopen(path, "ab")` — INSERT 시 **반드시 binary mode**. text mode 로 열면 Windows 에서 오프셋이 틀어진다.
2. `fetch_by_offset` 는 `fopen(path, "rb")` 로 열어야 `fseek` 오프셋이 일치한다.
3. `executor_run` 은 기존 시그니처를 유지한다 (main.c 가 호출하므로).
4. 선형 스캔 경로(`linear_scan`)는 제거하지 않는다 — `WHERE name = ?` 같은 비인덱스 쿼리에서 계속 사용된다.
5. 시간 측정 출력은 `stderr` 에만 한다. `stdout` 에는 결과 테이블만 출력한다.
