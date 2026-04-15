# SQL Parser with B+ Tree Index

간단한 SQL 처리기에 B+ 트리 인덱스를 추가해 대용량 데이터에서의 SELECT/INSERT 성능을 비교한다.

---

## 팀 구성 및 담당

| 역할 | 담당자 | 소유 파일 | 문서 |
|------|--------|----------|------|
| A — B+ Tree 알고리즘 | 김용 | `include/bptree.h`, `src/bptree/bptree.c` | [docs/role_A_김용.md](docs/role_A_김용.md) |
| B — 인덱스 매니저 | 김은재 | `include/index_manager.h`, `src/index/index_manager.c` | [docs/role_B_김은재.md](docs/role_B_김은재.md) |
| C — SQL 파서 확장 | 김규민 | `src/input/lexer.c`, `src/parser/parser.c`, `src/schema/schema.c` | [docs/role_C_김규민.md](docs/role_C_김규민.md) |
| D — Executor + 성능 | 김원우 | `src/executor/executor.c`, `src/main.c` | [docs/role_D_김원우.md](docs/role_D_김원우.md) |

**공유 파일 (수정 시 전원 합의 필요)**: `include/interface.h`, `Makefile`

---

## 아키텍처

```
SQL 파일
   │
   ▼
[Lexer] ──────── TOKEN_BETWEEN / TOKEN_AND 추가 (C: 김규민)
   │
   ▼
[Parser] ─────── WHERE col BETWEEN a AND b 지원 (C: 김규민)
   │
   ▼
[Schema] ─────── BETWEEN 컬럼 타입 검증 (C: 김규민)
   │
   ├──[INSERT]──▶ db_insert ──▶ 파일 append + B+ Tree 등록 (D: 김원우)
   │                                    │
   │                              [Index Manager] (B: 김은재)
   │                                    │
   │                             [B+ Tree #1: id]      (A: 김용)
   │                             [B+ Tree #2: id,age]  (A: 김용)
   │
   └──[SELECT]──▶ db_select ──▶ WHERE id=?              → Tree #1 point search
                                WHERE id BETWEEN a AND b → Tree #1 range search
                                WHERE name=? 등          → 선형 스캔 (비교군)
                                각 경로 실행 시간 + 트리 높이 출력 (D: 김원우)
```

---

## 빌드

```bash
make          # 기본 빌드 → ./sqlp
make sim      # 디스크 I/O 시뮬레이션 빌드 → ./sqlp_sim
make perf     # 성능 비교 실행 파일 → ./test_perf
make perf_sim # 성능 비교 + I/O 시뮬레이션 → ./test_perf_sim
make gen_data # 데이터 생성기 → ./gen_data
make test     # 단위 테스트 실행
make clean    # 빌드 결과물 삭제
```

---

## 실행

```bash
# SQL 파일 실행
./sqlp samples/insert.sql

# 대용량 데이터 생성 (100만 건)
./gen_data 1000000 users
./sqlp samples/bench_users.sql   # 삽입 실행

# 성능 비교
./test_perf
./test_perf_sim   # I/O 시뮬레이션 포함
```

---

## 지원 SQL 문법

```sql
INSERT INTO users VALUES (1, 'alice', 25, 'alice@example.com');
INSERT INTO users (id, name, age, email) VALUES (2, 'bob', 30, 'bob@example.com');

SELECT * FROM users;
SELECT * FROM users WHERE id = 42;
SELECT * FROM users WHERE name = 'alice';
SELECT * FROM users WHERE id BETWEEN 100 AND 200;
```

---

## 스키마 (`schema/users.schema`)

```
col0=id,INT,0         ← B+ Tree #1 인덱스 키
col1=name,VARCHAR,64
col2=age,INT,0        ← B+ Tree #2 복합 키 (id, age)
col3=email,VARCHAR,128
```

---

## 성능 출력 예시

```
[SELECT][index:id:eq         ]    0.012 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][index:id:range      ]    0.087 ms  tree_h(id)=4  tree_h(comp)=4
[SELECT][linear              ]  342.110 ms  tree_h(id)=4  tree_h(comp)=4
```

`make sim` 으로 빌드하면 레벨 이동마다 200µs sleep 이 추가되어 높이별 시간 차이가 명확해진다.

---

## 파일 구조

```
.
├── include/
│   ├── interface.h           ← 공유 계약 (전원 합의 필요)
│   ├── bptree.h              ← A (김용)
│   └── index_manager.h      ← B (김은재)
├── src/
│   ├── main.c                ← D (김원우)
│   ├── bptree/bptree.c       ← A (김용)
│   ├── index/index_manager.c ← B (김은재)
│   ├── input/{input,lexer}.c ← C (김규민)
│   ├── parser/parser.c       ← C (김규민)
│   ├── schema/schema.c       ← C (김규민)
│   └── executor/executor.c   ← D (김원우)
├── tests/                    ← 공통 (공동 작업)
├── tools/gen_data.c          ← 공통
├── schema/users.schema
├── docs/role_{A,B,C,D}_*.md
└── Makefile
```
