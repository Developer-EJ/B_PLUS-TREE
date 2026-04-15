/*
 * test_index.c — 인덱스 매니저 단위 / 기능 / 엣지 케이스 테스트
 *
 * 역할 B (김은재) 담당 파일:
 *   include/index_manager.h, src/index/index_manager.c
 *
 * 테스트 범주:
 *   1. 단위 테스트  — init, insert/search(id), range(id), insert/range(age),
 *                     height, cleanup, IO 통계
 *   2. 기능 테스트  — .dat 파일 자동 적재, 중복 age, 범위 경계 포함
 *   3. 엣지 케이스  — 미초기화 테이블, 빈 범위, 테이블 최대 개수 초과,
 *                     cleanup 후 재초기화, range_alloc API
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#  include <direct.h>
#  define MKDIR(p) _mkdir(p)
#else
#  include <sys/stat.h>
#  define MKDIR(p) mkdir(p, 0755)
#endif

#include "../include/interface.h"
#include "../include/index_manager.h"

/* 다른 테스트 테이블과 충돌을 피하기 위한 전용 이름 */
#define TEST_TABLE "test_idx_users"

static int failures  = 0;
static int tests_run = 0;

#define EXPECT_TRUE(cond, msg) do {                                       \
    if (!(cond)) {                                                        \
        fprintf(stderr, "[FAIL] %s:%d: %s\n", __FILE__, __LINE__, (msg)); \
        failures++;                                                       \
        goto cleanup;                                                     \
    }                                                                     \
} while (0)

static void run_test(const char *name, void (*fn)(void)) {
    int before = failures;
    fn();
    tests_run++;
    if (failures == before)
        printf("[PASS] %s\n", name);
}

/* ================================================================
 * 픽스처 헬퍼
 * ================================================================ */

static void build_data_path(char *buf, size_t size) {
    snprintf(buf, size, "data/%s.dat", TEST_TABLE);
}

static void remove_if_exists(const char *path) {
    if (path) remove(path);
}

/* .dat 파일에 행 하나를 binary 모드로 기록하고,
 * 그 행의 파일 시작 오프셋(index_manager 가 저장하는 값)을 반환한다. */
static long write_dat_row(FILE *fp,
                          int id, const char *name,
                          int age, const char *email) {
    long offset = ftell(fp);
    fprintf(fp, "%d | %s | %d | %s\n", id, name, age, email);
    return offset;
}

/*
 * 테스트 전: index_cleanup() + .dat 삭제 후
 * "data/" 디렉토리와 index 슬롯을 초기화한다.
 */
static void reset_fixture(void) {
    char data_path[256];
    build_data_path(data_path, sizeof(data_path));
    index_cleanup();
    remove_if_exists(data_path);
    MKDIR("data");
}

/* 테스트 후: index 슬롯과 .dat 파일을 모두 정리한다. */
static void cleanup_fixture(void) {
    char data_path[256];
    build_data_path(data_path, sizeof(data_path));
    index_cleanup();
    remove_if_exists(data_path);
}

/* ================================================================
 * 1. 단위 테스트 — init
 * ================================================================ */

/* .dat 파일이 없어도 index_init() 은 0을 반환해야 한다. */
static void test_init_no_dat_file_succeeds(void) {
    int result;
    reset_fixture();
    result = index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);
    EXPECT_TRUE(result == 0,
                "index_init should succeed even when .dat file is missing");
cleanup:
    cleanup_fixture();
}

/* 같은 테이블로 index_init() 을 두 번 호출해도 0을 반환해야 한다(멱등). */
static void test_init_idempotent(void) {
    int r1;
    int r2;
    reset_fixture();
    r1 = index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);
    r2 = index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);
    EXPECT_TRUE(r1 == 0, "first index_init should return 0");
    EXPECT_TRUE(r2 == 0, "second index_init (idempotent) should return 0");
cleanup:
    cleanup_fixture();
}

/* ================================================================
 * 1. 단위 테스트 — insert / search (id 인덱스)
 * ================================================================ */

/* index_insert_id 후 index_search_id 는 삽입한 offset을 반환해야 한다. */
static void test_insert_id_and_search(void) {
    long result;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    EXPECT_TRUE(index_insert_id(TEST_TABLE, 42, 1000L) == 0,
                "index_insert_id should return 0");
    result = index_search_id(TEST_TABLE, 42);
    EXPECT_TRUE(result == 1000L,
                "index_search_id should return the inserted offset");
cleanup:
    cleanup_fixture();
}

/* 삽입하지 않은 id 를 검색하면 -1이 반환되어야 한다. */
static void test_search_missing_id_returns_neg1(void) {
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    index_insert_id(TEST_TABLE, 1, 100L);
    EXPECT_TRUE(index_search_id(TEST_TABLE, 999) == -1,
                "search for absent id should return -1");
cleanup:
    cleanup_fixture();
}

/* 초기화하지 않은 테이블 이름으로 검색하면 -1이 반환되어야 한다. */
static void test_search_uninitialized_table_returns_neg1(void) {
    reset_fixture();
    /* "unknown_table" 은 index_init 을 호출한 적 없다. */
    EXPECT_TRUE(index_search_id("unknown_table", 1) == -1,
                "search on uninitialized table should return -1");
cleanup:
    cleanup_fixture();
}

/* ================================================================
 * 1. 단위 테스트 — range (id 인덱스)
 * ================================================================ */

/* id 1~5 를 삽입하고 range [1, 5] 를 조회하면 5개가 반환되어야 한다. */
static void test_range_id_basic(void) {
    long offsets[10];
    int  i;
    int  count;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    for (i = 1; i <= 5; i++)
        index_insert_id(TEST_TABLE, i, (long)(i * 100));
    count = index_range_id(TEST_TABLE, 1, 5, offsets, 10);
    EXPECT_TRUE(count == 5, "range [1,5] should return 5 results");
cleanup:
    cleanup_fixture();
}

/* 삽입된 키 범위 밖의 range 쿼리는 0을 반환해야 한다. */
static void test_range_id_empty_returns_zero(void) {
    long offsets[10];
    int  count;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    index_insert_id(TEST_TABLE, 1, 100L);
    index_insert_id(TEST_TABLE, 2, 200L);
    count = index_range_id(TEST_TABLE, 50, 100, offsets, 10);
    EXPECT_TRUE(count == 0, "range outside all keys should return 0");
cleanup:
    cleanup_fixture();
}

/* range [lo, hi] 는 lo 와 hi 양 끝을 포함해야 한다. */
static void test_range_id_boundary_inclusive(void) {
    long offsets[10];
    int  i;
    int  count;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    for (i = 1; i <= 10; i++)
        index_insert_id(TEST_TABLE, i, (long)(i * 100));
    count = index_range_id(TEST_TABLE, 3, 7, offsets, 10);
    EXPECT_TRUE(count == 5,
                "range [3,7] should include both boundary ids (3 and 7)");
cleanup:
    cleanup_fixture();
}

/* ================================================================
 * 1. 단위 테스트 — insert / range (age 인덱스)
 * ================================================================ */

/* index_insert_age 후 index_range_age 는 해당 offset을 반환해야 한다. */
static void test_insert_age_and_range(void) {
    long offsets[10];
    int  count;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    index_insert_age(TEST_TABLE, 25, 500L);
    count = index_range_age(TEST_TABLE, 20, 30, offsets, 10);
    EXPECT_TRUE(count == 1,
                "range_age [20,30] should return 1 result for age=25");
    EXPECT_TRUE(offsets[0] == 500L,
                "range_age should return the inserted offset");
cleanup:
    cleanup_fixture();
}

/*
 * 같은 age 로 두 행이 삽입된 경우,
 * range_age 쿼리는 두 offset을 모두 반환해야 한다.
 */
static void test_age_range_duplicate_age(void) {
    long offsets[10];
    int  count;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    index_insert_age(TEST_TABLE, 30, 100L);
    index_insert_age(TEST_TABLE, 30, 200L);
    count = index_range_age(TEST_TABLE, 28, 32, offsets, 10);
    EXPECT_TRUE(count == 2,
                "duplicate age should yield 2 offsets in range_age result");
cleanup:
    cleanup_fixture();
}

/* ================================================================
 * 1. 단위 테스트 — height
 * ================================================================ */

/* 행을 여러 개 삽입한 뒤 id/age 트리의 높이는 1 이상이어야 한다. */
static void test_height_positive_after_inserts(void) {
    int i;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    for (i = 1; i <= 10; i++) {
        index_insert_id(TEST_TABLE, i, (long)(i * 100));
        index_insert_age(TEST_TABLE, 20 + i, (long)(i * 100));
    }
    EXPECT_TRUE(index_height_id(TEST_TABLE) >= 1,
                "id tree height should be at least 1 after inserts");
    EXPECT_TRUE(index_height_age(TEST_TABLE) >= 1,
                "age tree height should be at least 1 after inserts");
cleanup:
    cleanup_fixture();
}

/* ================================================================
 * 1. 단위 테스트 — IO 통계
 * ================================================================ */

/* index_search_id() 호출 후 index_last_io_id() 는 1 이상이어야 한다. */
static void test_last_io_id_positive_after_search(void) {
    int i;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_SMALL, IDX_ORDER_SMALL) == 0,
                "index_init should succeed");
    /* order=4(IDX_ORDER_SMALL)로 많은 키를 삽입해 다단 트리를 만든다. */
    for (i = 1; i <= 30; i++)
        index_insert_id(TEST_TABLE, i, (long)(i * 100));
    index_search_id(TEST_TABLE, 15);
    EXPECT_TRUE(index_last_io_id(TEST_TABLE) >= 1,
                "last_io_id should be >= 1 after a search on a multi-level tree");
cleanup:
    cleanup_fixture();
}

/* index_reset_io_stats() 호출 후 last_io 는 0이어야 한다. */
static void test_reset_io_stats(void) {
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    index_insert_id(TEST_TABLE, 1, 100L);
    index_search_id(TEST_TABLE, 1);    /* last_io 가 기록된다 */
    index_reset_io_stats(TEST_TABLE);
    EXPECT_TRUE(index_last_io_id(TEST_TABLE) == 0,
                "index_reset_io_stats should set last_io_id to 0");
    EXPECT_TRUE(index_last_io_age(TEST_TABLE) == 0,
                "index_reset_io_stats should set last_io_age to 0");
cleanup:
    cleanup_fixture();
}

/* ================================================================
 * 1. 단위 테스트 — cleanup 후 재초기화
 * ================================================================ */

/* index_cleanup() 후 같은 테이블로 재초기화해도 정상 동작해야 한다. */
static void test_cleanup_and_reinit(void) {
    long result;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "first index_init should succeed");
    index_insert_id(TEST_TABLE, 7, 777L);

    index_cleanup();

    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "reinit after cleanup should succeed");
    /* cleanup 후에는 이전에 삽입한 키가 없어야 한다(파일 없으므로 빈 인덱스). */
    result = index_search_id(TEST_TABLE, 7);
    EXPECT_TRUE(result == -1,
                "after cleanup+reinit (no .dat file), old key should not exist");
cleanup:
    cleanup_fixture();
}

/* ================================================================
 * 2. 기능 테스트 — .dat 파일 자동 적재
 * ================================================================ */

/*
 * .dat 파일에 행을 미리 기록한 뒤 index_init() 을 호출하면
 * 파일을 스캔해 인덱스를 자동으로 구성해야 한다.
 */
static void test_init_loads_dat_file(void) {
    char  data_path[256];
    FILE *fp = NULL;
    long  offset_id1 = -1;
    long  offset_id2 = -1;
    long  found;
    reset_fixture();
    build_data_path(data_path, sizeof(data_path));

    /* binary 모드로 .dat 파일 작성 */
    fp = fopen(data_path, "wb");
    EXPECT_TRUE(fp != NULL, "should be able to create test .dat file");
    offset_id1 = write_dat_row(fp, 1, "alice", 25, "alice@example.com");
    offset_id2 = write_dat_row(fp, 2, "bob",   30, "bob@example.com");
    fclose(fp);
    fp = NULL;

    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed with existing .dat file");

    found = index_search_id(TEST_TABLE, 1);
    EXPECT_TRUE(found == offset_id1,
                "id=1 should map to the offset of the first line");

    found = index_search_id(TEST_TABLE, 2);
    EXPECT_TRUE(found == offset_id2,
                "id=2 should map to the offset of the second line");

cleanup:
    if (fp) fclose(fp);
    cleanup_fixture();
}

/*
 * .dat 파일에 같은 age 를 가진 행이 여러 개 있을 때,
 * index_init() 이 로드 후 range_age 쿼리가 모든 offset을 반환해야 한다.
 */
static void test_init_loads_duplicate_ages(void) {
    char  data_path[256];
    FILE *fp = NULL;
    long  offsets[10];
    int   count;
    reset_fixture();
    build_data_path(data_path, sizeof(data_path));

    fp = fopen(data_path, "wb");
    EXPECT_TRUE(fp != NULL, "should be able to create test .dat file");
    write_dat_row(fp, 1, "alice", 25, "a@example.com");
    write_dat_row(fp, 2, "bob",   25, "b@example.com"); /* age 중복 */
    write_dat_row(fp, 3, "carol", 30, "c@example.com");
    fclose(fp);
    fp = NULL;

    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");

    count = index_range_age(TEST_TABLE, 25, 25, offsets, 10);
    EXPECT_TRUE(count == 2,
                "age=25 appears twice, range_age should return 2 offsets");

cleanup:
    if (fp) fclose(fp);
    cleanup_fixture();
}

/* ================================================================
 * 3. 엣지 케이스 — 테이블 최대 개수 초과
 * ================================================================ */

/*
 * IDX_MAX_TABLES 개를 초과해서 index_init() 을 호출하면 -1 이 반환되어야 한다.
 * (각 테이블 이름은 달라야 한다.)
 */
static void test_max_tables_exceeded(void) {
    char  tbl[64];
    int   i;
    int   result;
    /* 슬롯을 전부 채운다. */
    index_cleanup();
    for (i = 0; i < IDX_MAX_TABLES; i++) {
        snprintf(tbl, sizeof(tbl), "overflow_tbl_%d", i);
        result = index_init(tbl, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);
        EXPECT_TRUE(result == 0,
                    "init within limit should succeed");
    }
    /* IDX_MAX_TABLES + 1 번째 테이블: 슬롯 없음 → -1 반환 */
    snprintf(tbl, sizeof(tbl), "overflow_tbl_%d", IDX_MAX_TABLES);
    result = index_init(tbl, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT);
    EXPECT_TRUE(result == -1,
                "init beyond IDX_MAX_TABLES should return -1");
cleanup:
    index_cleanup();
}

/* ================================================================
 * 3. 엣지 케이스 — range_alloc API
 * ================================================================ */

/* index_range_id_alloc() 은 범위 내 모든 offset 을 동적 배열로 반환해야 한다. */
static void test_range_id_alloc_basic(void) {
    int   i;
    int   count = 0;
    long *offsets;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    for (i = 1; i <= 5; i++)
        index_insert_id(TEST_TABLE, i, (long)(i * 100));
    offsets = index_range_id_alloc(TEST_TABLE, 2, 4, &count);
    EXPECT_TRUE(count == 3,
                "range_id_alloc [2,4] should return count=3");
    EXPECT_TRUE(offsets != NULL,
                "range_id_alloc should return non-NULL for non-empty result");
cleanup:
    free(offsets);
    cleanup_fixture();
}

/* index_range_age_alloc() 은 범위 내 모든 offset 을 동적 배열로 반환해야 한다. */
static void test_range_age_alloc_basic(void) {
    int   count = 0;
    long *offsets;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    index_insert_age(TEST_TABLE, 25, 100L);
    index_insert_age(TEST_TABLE, 28, 200L);
    index_insert_age(TEST_TABLE, 35, 300L);
    offsets = index_range_age_alloc(TEST_TABLE, 24, 30, &count);
    EXPECT_TRUE(count == 2,
                "range_age_alloc [24,30] should return count=2 for ages 25 and 28");
    EXPECT_TRUE(offsets != NULL,
                "range_age_alloc should return non-NULL for non-empty result");
cleanup:
    free(offsets);
    cleanup_fixture();
}

/* 범위에 해당하는 키가 없을 때 range_id_alloc 은 NULL 을 반환해야 한다. */
static void test_range_id_alloc_empty_returns_null(void) {
    int   count = 99;
    long *offsets;
    reset_fixture();
    EXPECT_TRUE(index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) == 0,
                "index_init should succeed");
    index_insert_id(TEST_TABLE, 1, 100L);
    offsets = index_range_id_alloc(TEST_TABLE, 50, 100, &count);
    EXPECT_TRUE(offsets == NULL,
                "range_id_alloc with no matching keys should return NULL");
    EXPECT_TRUE(count == 0,
                "range_id_alloc with no matching keys should set count=0");
cleanup:
    free(offsets);
    cleanup_fixture();
}

/* ================================================================
 * main — 테스트 실행
 * ================================================================ */

int main(void) {
    /* 1. 단위 테스트 — init */
    run_test("init: succeeds without .dat file",      test_init_no_dat_file_succeeds);
    run_test("init: idempotent on double call",        test_init_idempotent);

    /* 1. 단위 테스트 — id 인덱스 insert / search */
    run_test("id: insert then search returns offset",  test_insert_id_and_search);
    run_test("id: search missing id returns -1",       test_search_missing_id_returns_neg1);
    run_test("id: search uninitialized table is -1",   test_search_uninitialized_table_returns_neg1);

    /* 1. 단위 테스트 — id range */
    run_test("id range: basic 5-key range",            test_range_id_basic);
    run_test("id range: outside all keys returns 0",   test_range_id_empty_returns_zero);
    run_test("id range: boundary endpoints included",  test_range_id_boundary_inclusive);

    /* 1. 단위 테스트 — age 인덱스 */
    run_test("age: insert then range returns offset",  test_insert_age_and_range);
    run_test("age: duplicate age returns 2 offsets",   test_age_range_duplicate_age);

    /* 1. 단위 테스트 — height */
    run_test("height: positive after inserts",         test_height_positive_after_inserts);

    /* 1. 단위 테스트 — IO 통계 */
    run_test("io: last_io_id positive after search",   test_last_io_id_positive_after_search);
    run_test("io: reset clears both io stats",         test_reset_io_stats);

    /* 1. 단위 테스트 — cleanup 재초기화 */
    run_test("cleanup: reinit gives empty index",      test_cleanup_and_reinit);

    /* 2. 기능 테스트 — .dat 파일 적재 */
    run_test("load: init scans .dat and builds index", test_init_loads_dat_file);
    run_test("load: duplicate ages loaded correctly",  test_init_loads_duplicate_ages);

    /* 3. 엣지 케이스 */
    run_test("edge: IDX_MAX_TABLES exceeded → -1",    test_max_tables_exceeded);
    run_test("edge: range_id_alloc basic",             test_range_id_alloc_basic);
    run_test("edge: range_age_alloc basic",            test_range_age_alloc_basic);
    run_test("edge: range_id_alloc empty → NULL",      test_range_id_alloc_empty_returns_null);

    if (failures > 0) {
        fprintf(stderr, "\n%d/%d index tests failed.\n", failures, tests_run);
        return 1;
    }

    printf("\nAll %d index tests passed.\n", tests_run);
    return 0;
}
