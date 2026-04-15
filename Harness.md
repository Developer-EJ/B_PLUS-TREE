# 하네스 엔지니어링 기법 요약

이 프로젝트에서 실제로 사용 중인 테스트/검증 하네스 기법들을 정리한다.

---

## 1. 스텁(Stub) 기반 병렬 개발

**적용 위치**: `src/bptree/bptree.c`, `src/index/index_manager.c`

인터페이스(헤더)를 Day 1에 확정하고, 구현 전에 스텁을 먼저 제공한다.  
스텁은 컴파일은 되고, 크래시 없이 안전한 기본값을 반환한다.

```c
// bptree.c — 실제 알고리즘 미구현, 컴파일만 가능한 상태
long bptree_search(BPTree *tree, int key) {
    if (!tree) return -1;
    (void)key;        // ← 미사용 경고 억제
    IO_SLEEP();
    return -1;        // ← 안전 기본값: "없음"
}

int bptree_insert(BPTree *tree, int key, long value) {
    if (!tree) return -1;
    (void)key; (void)value;
    return 0;         // ← 안전 기본값: "성공"
}
```

**효과**: 4명이 A의 B+ Tree 구현 완료를 기다리지 않고 Day 1부터 병렬 작업 가능.

---

## 2. 인터페이스 우선 설계 (Interface-First Contract)

**적용 위치**: `include/interface.h`, `include/bptree.h`, `include/index_manager.h`

구현 전에 함수 시그니처와 데이터 구조를 헤더에 먼저 확정한다.  
헤더가 곧 팀 계약(contract)이며, 변경 시 전원 합의가 필요하다.

```
include/
  interface.h      ← WhereClause, TokenType 등 공유 타입 정의
  bptree.h         ← A(김용)가 제공할 API 선언
  index_manager.h  ← B(김은재)가 제공할 API 선언
```

스텁 기반 병렬 개발이 가능한 전제 조건이다.

---

## 3. TODO 가드 패턴

**적용 위치**: `src/bptree/bptree.c`, `src/index/index_manager.c`

미구현 구간을 구조화된 TODO 주석으로 마킹해 구현 위치와 방법을 명확히 안내한다.

```c
int bptree_range(BPTree *tree, int from, int to,
                 long *out, int max_count) {
    /*
     * TODO (김용):
     *   1. from 에 해당하는 리프까지 내려간다.
     *   2. 리프 링크드 리스트를 따라 key <= to 인 동안 순회한다.
     *   3. 각 매칭 엔트리의 value 를 out[] 에 저장한다.
     */
    if (!tree || !out || max_count <= 0) return 0;
    (void)from; (void)to;
    return 0; /* 스텁 */
}
```

---

## 4. I/O 시뮬레이션 하네스

**적용 위치**: `src/bptree/bptree.c`, `include/bptree.h`

실제 디스크 I/O 없이도 "높이별 탐색 시간 차이"를 측정하기 위한 하네스.  
컴파일 플래그로 ON/OFF 전환한다.

```c
// bptree.c 내부
#if BPTREE_SIMULATE_IO
#  ifdef _WIN32
#    define IO_SLEEP() Sleep(DISK_IO_DELAY_US / 1000)
#  else
#    define IO_SLEEP() usleep(DISK_IO_DELAY_US)
#  endif
#else
#  define IO_SLEEP() ((void)0)   // 일반 빌드: sleep 없음
#endif

// 탐색 시 레벨 이동마다 호출
long bptree_search(BPTree *tree, int key) {
    // ... 레벨 이동 ...
    IO_SLEEP();  // ← 여기서 지연 삽입
}
```

```bash
make       # I/O 시뮬레이션 없음 (빠름)
make sim   # -DBPTREE_SIMULATE_IO=1 → 레벨당 200µs 지연
```

스텁 단계에서도 `IO_SLEEP()`이 호출되므로 시뮬레이션 경로를 미리 검증 가능.

---

## 5. 역할 분리 단위 테스트 구조

**적용 위치**: `Makefile`, `tests/`

각 역할의 코드를 독립적인 바이너리로 빌드해 테스트한다.  
다른 역할의 구현에 의존하지 않고 자신의 코드만 검증할 수 있다.

```makefile
# 역할 A만 링크
test_bptree: tests/test_bptree.c src/bptree/bptree.c
    $(CC) $(CFLAGS) -o $@ $^

# 역할 B는 A의 스텁을 링크해 독립 실행
test_index: tests/test_index.c src/bptree/bptree.c \
            src/index/index_manager.c src/schema/schema.c
    $(CC) $(CFLAGS) -o $@ $^
```

```
make test  →  test_bptree / test_index / test_parser / test_schema / test_executor
              각각 [PASS] / [FAIL] 출력
```

---

## 6. 성능 측정 하네스

**적용 위치**: `tests/test_perf.c`, `src/executor/executor.c`

별도 실행 파일(`test_perf`)로 성능 비교를 격리한다.  
`clock()`을 사용해 C99 표준 내에서 이식성 있는 시간 측정을 수행한다.

```c
// 공통 타이머 (C99 표준, Windows/Linux 모두 동작)
static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}

// 사용 패턴
double t0 = now_ms();
index_search_id(table, id);
double elapsed = now_ms() - t0;
```

실행 중 성능 출력도 하네스의 일부다:
```
[SELECT][index:id:eq         ]    0.012 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][linear              ]  342.110 ms  tree_h(id)=4  tree_h(comp)=4
```
- `stderr`: 타이밍/진단 출력 전용
- `stdout`: 결과 테이블 행 전용 (출력 채널 분리)

---

## 7. 데이터 생성 하네스

**적용 위치**: `tools/gen_data.c`

대용량 INSERT SQL을 자동 생성해 성능 테스트 데이터를 준비한다.

```bash
./gen_data 1000000 users   # samples/bench_users.sql 생성
./sqlp samples/bench_users.sql  # 100만 건 삽입
./test_perf users 1000000       # 성능 측정
```

수작업 데이터 준비 없이 재현 가능한 벤치마크 환경을 구성한다.

---

## 8. 프리커밋 검증 파이프라인

**적용 위치**: `.githooks/pre-commit`, `scripts/`

`git commit` 시 자동으로 세 단계 검증을 실행한다.

```
git commit
   ├─ [1/3] lint.sh           → gcc -fsyntax-only (컴파일 에러)
   ├─ [2/3] check_ownership.sh → 파일 소유권 위반 검사
   └─ [3/3] check_binary_mode.sh → fopen text mode 검사

실패 시 → 커밋 차단 + logs/pre_commit_failures.log 에 기록
성공 시 → 커밋 허용 + 타임스탬프 기록
```

컴파일러가 없는 환경에서는 린트를 SKIP하고 통과한다 (도구 부재가 커밋을 막지 않음).

---

## 9. 피드백 루프 로깅

**적용 위치**: `logs/pre_commit_failures.log`

프리커밋 실패 내용을 파일에 누적해 에이전트와 개발자가 참조할 수 있게 한다.

```
[2026-04-15 15:05:50] BLOCKED (1 failures)
Staged files:
  src/executor/executor.c

--- BINARY_MODE FAILED ---
273:    FILE *fp = fopen(path, "r");
→ "rb" / "ab" / "wb" 로 변경하세요
```

에이전트는 이 로그를 읽고 수정 후 재커밋한다.

---

## 10. 멱등성(Idempotency) 계약

**적용 위치**: `src/index/index_manager.c`

`index_init`은 같은 테이블로 여러 번 호출해도 트리가 중복 생성되지 않는다.  
`main.c`가 SQL 구문마다 `index_init`을 호출하는 구조에서 필수 조건이다.

```c
int index_init(const char *table, int order_id, int order_comp) {
    // 이미 초기화된 테이블이면 즉시 성공 반환
    if (find_entry(table)) return 0;
    // ... 실제 초기화 ...
}
```

---

## 기법 한눈에 보기

| # | 기법 | 목적 | 위치 |
|---|------|------|------|
| 1 | 스텁 기반 병렬 개발 | 의존성 없이 4명 동시 작업 | `bptree.c`, `index_manager.c` |
| 2 | 인터페이스 우선 설계 | 팀 계약, 스텁의 전제 조건 | `include/*.h` |
| 3 | TODO 가드 패턴 | 미구현 구간 명시 + 구현 가이드 | 스텁 함수 내부 |
| 4 | I/O 시뮬레이션 | 트리 높이별 시간 차이 측정 | `bptree.c`, `make sim` |
| 5 | 역할 분리 단위 테스트 | 독립적 검증, 타 역할 의존 배제 | `Makefile`, `tests/` |
| 6 | 성능 측정 하네스 | 선형 vs 인덱스 시간 비교 | `test_perf.c`, `executor.c` |
| 7 | 데이터 생성 하네스 | 재현 가능한 대용량 벤치마크 | `tools/gen_data.c` |
| 8 | 프리커밋 검증 파이프라인 | 잘못된 커밋 사전 차단 | `.githooks/`, `scripts/` |
| 9 | 피드백 루프 로깅 | 실패 원인 기록 + 에이전트 참조 | `logs/` |
| 10 | 멱등성 계약 | 중복 호출 안전 보장 | `index_manager.c` |
