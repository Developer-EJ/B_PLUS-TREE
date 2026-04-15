CC     = gcc
CFLAGS = -std=c99 -Wall -Wextra -Iinclude

# ── 메인 빌드 소스 ─────────────────────────────────────────
SRCS = src/main.c            \
       src/input/input.c     \
       src/input/lexer.c     \
       src/parser/parser.c   \
       src/schema/schema.c   \
       src/executor/executor.c \
       src/bptree/bptree.c   \
       src/index/index_manager.c

TARGET = sqlp

# ── 기본 빌드 ──────────────────────────────────────────────
all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^

# ── 디스크 I/O 시뮬레이션 빌드 (B+ 트리 높이별 시간 비교용) ──
sim: $(SRCS)
	$(CC) $(CFLAGS) -DBPTREE_SIMULATE_IO=1 -o sqlp_sim $^

# ── 성능 비교 실행 파일 (별도 main) ───────────────────────
PERF_SRCS = tests/test_perf.c       \
            src/input/input.c       \
            src/input/lexer.c       \
            src/parser/parser.c     \
            src/schema/schema.c     \
            src/executor/executor.c \
            src/bptree/bptree.c     \
            src/index/index_manager.c

perf: $(PERF_SRCS)
	$(CC) $(CFLAGS) -o test_perf $^

perf_sim: $(PERF_SRCS)
	$(CC) $(CFLAGS) -DBPTREE_SIMULATE_IO=1 -o test_perf_sim $^

# ── 데이터 생성기 ──────────────────────────────────────────
gen_data: tools/gen_data.c
	$(CC) $(CFLAGS) -o gen_data $^

# ── 단위 테스트 ────────────────────────────────────────────
TEST_BINS = test_bptree test_index test_parser test_schema test_executor

test: $(TEST_BINS)
	@echo ""
	@echo "========== Running Tests =========="
	@./test_bptree   && echo "[PASS] bptree"   || echo "[FAIL] bptree"
	@./test_index    && echo "[PASS] index"    || echo "[FAIL] index"
	@./test_parser   && echo "[PASS] parser"   || echo "[FAIL] parser"
	@./test_schema   && echo "[PASS] schema"   || echo "[FAIL] schema"
	@./test_executor && echo "[PASS] executor" || echo "[FAIL] executor"
	@echo "==================================="

# 역할 A (김용) — B+ Tree 단위 테스트
test_bptree: tests/test_bptree.c \
             src/bptree/bptree.c
	$(CC) $(CFLAGS) -o $@ $^

# 역할 B (김은재) — 인덱스 매니저 단위 테스트
test_index: tests/test_index.c   \
            src/bptree/bptree.c  \
            src/index/index_manager.c \
            src/schema/schema.c
	$(CC) $(CFLAGS) -o $@ $^

# 역할 C (김규민) — 파서 단위 테스트
test_parser: tests/test_parser.c  \
             src/input/input.c    \
             src/input/lexer.c    \
             src/parser/parser.c
	$(CC) $(CFLAGS) -o $@ $^

# 역할 C (김규민) — 스키마 단위 테스트
test_schema: tests/test_schema.c \
             src/schema/schema.c
	$(CC) $(CFLAGS) -o $@ $^

# 역할 D (김원우) — Executor 단위 테스트
test_executor: tests/test_executor.c  \
               src/schema/schema.c    \
               src/executor/executor.c \
               src/bptree/bptree.c    \
               src/index/index_manager.c
	$(CC) $(CFLAGS) -o $@ $^

# ── 정리 ───────────────────────────────────────────────────
clean:
	rm -f $(TARGET) sqlp_sim $(TEST_BINS) test_perf test_perf_sim gen_data

.PHONY: all sim perf perf_sim gen_data test clean
