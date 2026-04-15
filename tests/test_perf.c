/* =========================================================
 * test_perf.c — 성능 비교 테스트 (별도 실행 파일)
 *
 * 공통 작업 파일 (담당자 지정 없음)
 *
 * 빌드:
 *   make perf       → ./test_perf      (일반)
 *   make perf_sim   → ./test_perf_sim  (I/O 시뮬레이션)
 *
 * 측정 항목:
 *   1. 선형 스캔 vs B+ 트리 (id=?) point search 시간
 *   2. 선형 스캔 vs B+ 트리 range search (id BETWEEN a AND b) 시간
 *   3. order=4 (높은 트리) vs order=128 (낮은 트리) 탐색 시간
 *   4. (2순위) 선형 INSERT vs B+ 트리 INSERT 시간
 * ========================================================= */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../include/interface.h"
#include "../include/index_manager.h"

/* ── 시간 측정 ──────────────────────────────────────────── */
static double now_ms(void) {
    return (double)clock() * 1000.0 / (double)CLOCKS_PER_SEC;
}

/* ── 결과 출력 헬퍼 ─────────────────────────────────────── */
static void print_result_row(const char *label, double ms, int rows,
                              int tree_h) {
    printf("  %-35s %10.3f ms  rows=%-8d  tree_h=%d\n",
           label, ms, rows, tree_h);
}

static void print_separator(void) {
    printf("  %s\n",
           "-------------------------------------------------------------------"
           "------");
}

/* =========================================================
 * TODO: 아래 각 함수에 실제 측정 코드를 작성한다.
 *
 * 구현 가이드:
 *   - SelectStmt 를 직접 생성해 db_select() 를 호출한다.
 *   - 또는 SQL 파일을 만들어 sqlp 를 호출하고 시간을 측정한다.
 *   - 각 측정은 최소 3회 반복해 평균을 낸다.
 * ========================================================= */

/* 테스트 1: point search — index vs linear */
static void bench_point_search(const char *table, int target_id) {
    printf("\n[Test 1] Point Search: WHERE id = %d\n", target_id);
    print_separator();

    /* TODO: index_search_id 로 point search 시간 측정 */
    double t0 = now_ms();
    long offset = index_search_id(table, target_id);
    double t1 = now_ms();
    print_result_row("B+ Tree #1 (id, point)",
                     t1 - t0,
                     offset >= 0 ? 1 : 0,
                     index_height_id(table));

    /* TODO: 선형 스캔 시간 측정 (data/{table}.dat 전체 탐색) */
    /* SelectStmt 생성 후 linear_scan 함수를 직접 호출하거나
     * db_select 를 사용해 측정한다. */
    printf("  %-35s  (TODO: 선형 스캔 구현 필요)\n",
           "Linear scan");

    print_separator();
}

/* 테스트 2: range search — index vs linear */
static void bench_range_search(const char *table, int from, int to) {
    printf("\n[Test 2] Range Search: WHERE id BETWEEN %d AND %d\n",
           from, to);
    print_separator();

    /* TODO: index_range_id 로 range search 시간 측정 */
    long *offsets = (long *)calloc(IDX_MAX_RANGE, sizeof(long));
    if (!offsets) { printf("  out of memory\n"); return; }

    double t0 = now_ms();
    int n = index_range_id(table, from, to, offsets, IDX_MAX_RANGE);
    double t1 = now_ms();
    free(offsets);

    print_result_row("B+ Tree #1 (id, range)",
                     t1 - t0, n, index_height_id(table));

    /* TODO: 선형 스캔 시간 측정 */
    printf("  %-35s  (TODO: 선형 스캔 구현 필요)\n",
           "Linear scan");

    print_separator();
}

/* 테스트 3: 트리 높이별 시간 비교 */
static void bench_height_comparison(const char *table, int target_id) {
    printf("\n[Test 3] Height Comparison: order=4 vs order=128\n");
    print_separator();

    /* order=4 (높은 트리) */
    index_cleanup();
    index_init(table, IDX_ORDER_SMALL, IDX_ORDER_SMALL);
    double t0 = now_ms();
    index_search_id(table, target_id);
    double t1 = now_ms();
    print_result_row("order=4  (tall tree)",
                     t1 - t0, 1, index_height_id(table));

    /* order=128 (낮은 트리) */
    index_cleanup();
    index_init(table, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);
    t0 = now_ms();
    index_search_id(table, target_id);
    t1 = now_ms();
    print_result_row("order=128 (flat tree)",
                     t1 - t0, 1, index_height_id(table));

    print_separator();
    printf("  ※ make perf_sim 으로 빌드하면 I/O 시뮬레이션이 활성화되어\n"
           "    높이별 시간 차이가 명확하게 나타납니다.\n");
}

/* 테스트 4: INSERT 속도 — 순수 파일 vs 파일+인덱스 (2순위) */
static void bench_insert(const char *table, int n) {
    printf("\n[Test 4] INSERT Speed: %d rows\n", n);
    print_separator();
    printf("  (TODO: 2순위 — 추후 구현)\n");
    (void)table; (void)n;
    print_separator();
}

/* =========================================================
 * main
 * ========================================================= */
int main(int argc, char *argv[]) {
    const char *table = (argc > 1) ? argv[1] : "users";
    int         rows  = (argc > 2) ? atoi(argv[2]) : 1000000;

    printf("============================================================\n");
    printf("  B+ Tree Performance Benchmark\n");
    printf("  Table: %s  |  Rows: %d\n", table, rows);
#if BPTREE_SIMULATE_IO
    printf("  Mode: Disk I/O Simulation ON (%d µs/level)\n",
           DISK_IO_DELAY_US);
#else
    printf("  Mode: In-Memory (no I/O simulation)\n");
#endif
    printf("============================================================\n");

    /* 인덱스 초기화 (기존 .dat 파일 스캔) */
    printf("\nInitializing index for '%s'...\n", table);
    if (index_init(table, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0) {
        fprintf(stderr, "index_init failed. "
                "Run ./sqlp samples/bench_%s.sql first.\n", table);
        return 1;
    }
    printf("  tree_h(id)=%d  tree_h(comp)=%d\n",
           index_height_id(table), index_height_comp(table));

    /* 벤치마크 실행 */
    bench_point_search(table, rows / 2);              /* 중간 id */
    bench_range_search(table, rows / 4, rows / 2);    /* 25%~50% */
    bench_height_comparison(table, rows / 2);
    bench_insert(table, 10000);

    printf("\n============================================================\n");
    printf("  Done.\n");
    printf("============================================================\n");

    index_cleanup();
    return 0;
}
