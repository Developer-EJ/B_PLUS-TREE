/*
 * test_bptree.c — B+ Tree 단위 / 기능 / 엣지 케이스 테스트
 *
 * 역할 A (김용) 담당 파일:
 *   include/bptree.h, src/bptree/bptree.c
 *
 * 테스트 범주:
 *   1. 단위 테스트  — create/destroy, insert/search, range, height
 *   2. 기능 테스트  — 순차/역순 삽입, 분할 후 정합성, 범위 쿼리 정확성
 *   3. 엣지 케이스  — NULL 인자, 중복 키, 빈 트리, 경계값, 대량 삽입
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/bptree.h"

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
 * 1. 단위 테스트 — create / destroy
 * ================================================================ */

/* bptree_create() 가 non-NULL 을 반환하고 초기 높이가 1이어야 한다. */
static void test_create_nonnull(void) {
    BPTree *tree = bptree_create(4);
    EXPECT_TRUE(tree != NULL, "bptree_create should return non-null");
    EXPECT_TRUE(bptree_height(tree) == 1, "initial height should be 1");
cleanup:
    bptree_destroy(tree);
}

/* order < 3 이면 내부에서 3으로 보정된 채 정상 생성되어야 한다. */
static void test_create_order_min_correction(void) {
    BPTree *tree = bptree_create(1);
    EXPECT_TRUE(tree != NULL,
                "bptree_create(1) should succeed after correction to order=3");
    EXPECT_TRUE(bptree_height(tree) >= 1,
                "corrected tree should have valid height");
cleanup:
    bptree_destroy(tree);
}

/* order = 0 도 보정되어 정상 생성되어야 한다. */
static void test_create_order_zero_correction(void) {
    BPTree *tree = bptree_create(0);
    EXPECT_TRUE(tree != NULL,
                "bptree_create(0) should succeed after correction to order=3");
cleanup:
    bptree_destroy(tree);
}

/* bptree_destroy(NULL) 은 crash 없이 무시되어야 한다. */
static void test_destroy_null_safe(void) {
    bptree_destroy(NULL); /* crash가 나지 않으면 성공 */
    EXPECT_TRUE(1, "bptree_destroy(NULL) must not crash");
cleanup:
    ;
}

/* ================================================================
 * 1. 단위 테스트 — insert / search
 * ================================================================ */

/* bptree_insert() 는 성공 시 0을 반환해야 한다. */
static void test_insert_returns_zero(void) {
    BPTree *tree = bptree_create(4);
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    EXPECT_TRUE(bptree_insert(tree, 42, 100L) == 0,
                "insert should return 0 on success");
cleanup:
    bptree_destroy(tree);
}

/* NULL 트리에 insert 하면 0이 아닌 값을 반환해야 한다. */
static void test_insert_null_tree_fails(void) {
    int result = bptree_insert(NULL, 42, 100L);
    EXPECT_TRUE(result != 0,
                "insert on NULL tree should return non-zero error");
cleanup:
    ;
}

/* 삽입한 키를 search 하면 삽입한 offset이 그대로 반환되어야 한다. */
static void test_search_returns_inserted_offset(void) {
    BPTree *tree = bptree_create(4);
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    EXPECT_TRUE(bptree_insert(tree, 7, 200L) == 0,
                "insert should succeed");
    EXPECT_TRUE(bptree_search(tree, 7) == 200L,
                "search should return the inserted offset");
cleanup:
    bptree_destroy(tree);
}

/* 삽입하지 않은 키를 search 하면 -1이 반환되어야 한다. */
static void test_search_missing_key_returns_neg1(void) {
    BPTree *tree = bptree_create(4);
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    EXPECT_TRUE(bptree_insert(tree, 5, 100L) == 0, "insert should succeed");
    EXPECT_TRUE(bptree_search(tree, 99) == -1,
                "search for absent key should return -1");
cleanup:
    bptree_destroy(tree);
}

/* 빈 트리에서 search 하면 -1이 반환되어야 한다. */
static void test_search_empty_tree_returns_neg1(void) {
    BPTree *tree = bptree_create(4);
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    EXPECT_TRUE(bptree_search(tree, 1) == -1,
                "search on empty tree should return -1");
cleanup:
    bptree_destroy(tree);
}

/* NULL 트리에서 search 하면 -1이 반환되어야 한다. */
static void test_search_null_tree_returns_neg1(void) {
    EXPECT_TRUE(bptree_search(NULL, 1) == -1,
                "search on NULL tree should return -1");
cleanup:
    ;
}

/* ================================================================
 * 1. 단위 테스트 — range
 * ================================================================ */

/* 빈 트리에서 range 쿼리를 하면 0이 반환되어야 한다. */
static void test_range_empty_tree_returns_zero(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    count = bptree_range(tree, 1, 5, offsets, 10);
    EXPECT_TRUE(count == 0, "range on empty tree should return 0");
cleanup:
    bptree_destroy(tree);
}

/* NULL 트리에서 range 쿼리를 하면 0이 반환되어야 한다. */
static void test_range_null_tree_returns_zero(void) {
    long offsets[10];
    int  count = bptree_range(NULL, 1, 5, offsets, 10);
    EXPECT_TRUE(count == 0, "range on NULL tree should return 0");
cleanup:
    ;
}

/* from > to 이면 range 쿼리가 0을 반환해야 한다. */
static void test_range_from_gt_to_returns_zero(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    bptree_insert(tree, 3, 100L);
    count = bptree_range(tree, 5, 1, offsets, 10); /* from > to */
    EXPECT_TRUE(count == 0, "range where from > to should return 0");
cleanup:
    bptree_destroy(tree);
}

/* 1~5를 삽입하고 range [1, 5] 를 조회하면 정확히 5개가 반환되어야 한다. */
static void test_range_basic_count(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    i;
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 5; i++)
        bptree_insert(tree, i, (long)(i * 100));
    count = bptree_range(tree, 1, 5, offsets, 10);
    EXPECT_TRUE(count == 5, "range [1,5] with 5 inserted keys should return 5");
cleanup:
    bptree_destroy(tree);
}

/* ================================================================
 * 2. 기능 테스트 — range 정확성
 * ================================================================ */

/* range [lo, hi] 는 lo 와 hi 양 끝을 포함해야 한다. */
static void test_range_boundary_inclusive(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    bptree_insert(tree, 1, 100L);
    bptree_insert(tree, 3, 300L);
    bptree_insert(tree, 5, 500L);
    count = bptree_range(tree, 1, 5, offsets, 10);
    EXPECT_TRUE(count == 3, "range [1,5] should include both boundary keys");
cleanup:
    bptree_destroy(tree);
}

/* range [3, 5] 는 1, 2, 6, 7 같은 범위 밖의 키를 포함하지 않아야 한다. */
static void test_range_excludes_outside_keys(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    i;
    int    count;
    int    found3 = 0;
    int    found4 = 0;
    int    found5 = 0;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 7; i++)
        bptree_insert(tree, i, (long)(i * 100));
    count = bptree_range(tree, 3, 5, offsets, 10);
    EXPECT_TRUE(count == 3, "range [3,5] should return exactly 3 keys");
    for (i = 0; i < count; i++) {
        if (offsets[i] == 300L) found3 = 1;
        if (offsets[i] == 400L) found4 = 1;
        if (offsets[i] == 500L) found5 = 1;
    }
    EXPECT_TRUE(found3 && found4 && found5,
                "range [3,5] should contain offsets 300, 400, 500");
cleanup:
    bptree_destroy(tree);
}

/* range [x, x] 는 키 x 에 해당하는 결과만 반환해야 한다. */
static void test_range_single_key(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    bptree_insert(tree, 10, 999L);
    bptree_insert(tree, 20, 888L);
    count = bptree_range(tree, 10, 10, offsets, 10);
    EXPECT_TRUE(count == 1, "range [x,x] should return exactly 1 result");
    EXPECT_TRUE(offsets[0] == 999L,
                "single-key range should return the correct offset");
cleanup:
    bptree_destroy(tree);
}

/* 삽입된 키 범위 밖의 range 쿼리는 0을 반환해야 한다. */
static void test_range_no_results(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    bptree_insert(tree, 10, 100L);
    bptree_insert(tree, 20, 200L);
    count = bptree_range(tree, 50, 100, offsets, 10);
    EXPECT_TRUE(count == 0, "range outside all keys should return 0");
cleanup:
    bptree_destroy(tree);
}

/* ================================================================
 * 2. 기능 테스트 — 중복 키 (age 인덱스 시뮬레이션)
 * ================================================================ */

/*
 * 같은 키에 두 개의 offset이 삽입된 경우,
 * range 쿼리는 두 offset을 모두 반환해야 한다.
 */
static void test_duplicate_key_all_offsets_in_range(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    /* 같은 age(key=25)에 두 row 의 offset 삽입 */
    bptree_insert(tree, 25, 100L);
    bptree_insert(tree, 25, 200L);
    count = bptree_range(tree, 25, 25, offsets, 10);
    EXPECT_TRUE(count == 2,
                "duplicate key should yield 2 offsets in range result");
cleanup:
    bptree_destroy(tree);
}

/* 중복 키에서 search 는 유효한 offset(-1 이 아닌 값)을 반환해야 한다. */
static void test_duplicate_key_search_returns_valid(void) {
    BPTree *tree = bptree_create(4);
    long   result;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    bptree_insert(tree, 30, 500L);
    bptree_insert(tree, 30, 600L);
    result = bptree_search(tree, 30);
    EXPECT_TRUE(result >= 0,
                "search on duplicate key should return a valid (>=0) offset");
cleanup:
    bptree_destroy(tree);
}

/* ================================================================
 * 2. 기능 테스트 — 대량 삽입 및 분할 정합성
 * ================================================================ */

/* 1부터 100까지 순서대로 삽입해도 모든 키가 정확히 검색되어야 한다. */
static void test_sequential_insert_all_searchable(void) {
    BPTree *tree = bptree_create(4);
    int    i;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 100; i++)
        bptree_insert(tree, i, (long)(i * 10));
    for (i = 1; i <= 100; i++) {
        EXPECT_TRUE(bptree_search(tree, i) == (long)(i * 10),
                    "sequentially inserted key should be searchable");
    }
cleanup:
    bptree_destroy(tree);
}

/* 역순으로 삽입해도 모든 키가 정확히 검색되어야 한다. */
static void test_reverse_insert_all_searchable(void) {
    BPTree *tree = bptree_create(4);
    int    i;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 100; i >= 1; i--)
        bptree_insert(tree, i, (long)(i * 10));
    for (i = 1; i <= 100; i++) {
        EXPECT_TRUE(bptree_search(tree, i) == (long)(i * 10),
                    "reverse-inserted key should be searchable");
    }
cleanup:
    bptree_destroy(tree);
}

/*
 * order=3이면 key 2개가 차면 분할이 발생한다.
 * 10개 삽입 후 높이가 1보다 커져야 한다.
 */
static void test_height_increases_on_split(void) {
    BPTree *tree = bptree_create(3);
    int    i;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    EXPECT_TRUE(bptree_height(tree) == 1, "initial height should be 1");
    for (i = 1; i <= 10; i++)
        bptree_insert(tree, i, (long)(i * 100));
    EXPECT_TRUE(bptree_height(tree) > 1,
                "height should increase after multiple leaf splits");
cleanup:
    bptree_destroy(tree);
}

/*
 * order=3으로 50개를 삽입하면 많은 분할이 발생한다.
 * 분할 이후에도 모든 키가 정확히 검색되어야 한다.
 */
static void test_large_bulk_insert_small_order(void) {
    BPTree *tree = bptree_create(3);
    int    i;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 50; i++)
        bptree_insert(tree, i, (long)(i * 100));
    for (i = 1; i <= 50; i++) {
        EXPECT_TRUE(bptree_search(tree, i) == (long)(i * 100),
                    "key should remain searchable after many leaf/root splits");
    }
cleanup:
    bptree_destroy(tree);
}

/* ================================================================
 * 3. 엣지 케이스 — bptree_range_alloc
 * ================================================================ */

/* range_alloc 은 범위에 맞는 개수와 비-NULL 포인터를 반환해야 한다. */
static void test_range_alloc_correct_count(void) {
    BPTree *tree = bptree_create(4);
    int    i;
    int    count = 0;
    long  *offsets;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 10; i++)
        bptree_insert(tree, i, (long)(i * 50));
    offsets = bptree_range_alloc(tree, 3, 7, &count);
    EXPECT_TRUE(count == 5,
                "range_alloc [3,7] over keys 1-10 should return count=5");
    EXPECT_TRUE(offsets != NULL,
                "range_alloc should return non-NULL for non-empty result");
cleanup:
    free(offsets);
    bptree_destroy(tree);
}

/* NULL 트리에서 range_alloc 은 NULL 을 반환하고 count 를 0으로 설정해야 한다. */
static void test_range_alloc_null_tree_returns_null(void) {
    int   count  = 99;
    long *result = bptree_range_alloc(NULL, 1, 5, &count);
    EXPECT_TRUE(result == NULL,
                "range_alloc on NULL tree should return NULL");
    EXPECT_TRUE(count == 0,
                "range_alloc on NULL tree should set count to 0");
cleanup:
    free(result);
}

/* ================================================================
 * 3. 엣지 케이스 — IO 카운터
 * ================================================================ */

/*
 * search 전에 tree_io(last_io)가 0이어야 한다.
 * search 후에는 적어도 1 이상의 노드 방문이 기록되어야 한다.
 */
static void test_last_io_tracks_node_visits(void) {
    BPTree *tree = bptree_create(3); /* 작은 order → 여러 레벨 */
    int    i;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 20; i++)
        bptree_insert(tree, i, (long)(i * 10));
    /* search 호출 전 last_io 리셋 확인 */
    bptree_search(tree, 10);
    EXPECT_TRUE(bptree_last_io(tree) >= 1,
                "last_io should be at least 1 after a search");
cleanup:
    bptree_destroy(tree);
}

/* ================================================================
 * 3. 엣지 케이스 — 교차 범위 / 경계 바로 밖
 * ================================================================ */

/* range 에 max_count 제한이 있을 때 그 수 이상을 반환하지 않아야 한다. */
static void test_range_respects_max_count(void) {
    long   offsets[3]; /* 일부러 3 으로 제한 */
    BPTree *tree = bptree_create(4);
    int    i;
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 10; i++)
        bptree_insert(tree, i, (long)(i * 100));
    count = bptree_range(tree, 1, 10, offsets, 3);
    EXPECT_TRUE(count <= 3,
                "range should not exceed the supplied max_count");
cleanup:
    bptree_destroy(tree);
}

/* range [from, to] 에서 to 보다 1 큰 키는 포함되지 않아야 한다. */
static void test_range_to_is_exclusive_beyond(void) {
    long   offsets[10];
    BPTree *tree = bptree_create(4);
    int    i;
    int    count;
    EXPECT_TRUE(tree != NULL, "tree creation should succeed");
    for (i = 1; i <= 5; i++)
        bptree_insert(tree, i, (long)(i * 100));
    count = bptree_range(tree, 1, 3, offsets, 10);
    EXPECT_TRUE(count == 3,
                "range [1,3] should not include key 4 or 5");
cleanup:
    bptree_destroy(tree);
}

/* ================================================================
 * main — 테스트 실행
 * ================================================================ */

int main(void) {
    /* 1. 단위 테스트 — create / destroy */
    run_test("create: returns non-null tree",       test_create_nonnull);
    run_test("create: order=1 corrected to 3",      test_create_order_min_correction);
    run_test("create: order=0 corrected to 3",      test_create_order_zero_correction);
    run_test("destroy: NULL arg is safe",            test_destroy_null_safe);

    /* 1. 단위 테스트 — insert / search */
    run_test("insert: returns 0 on success",         test_insert_returns_zero);
    run_test("insert: NULL tree returns error",       test_insert_null_tree_fails);
    run_test("search: returns inserted offset",       test_search_returns_inserted_offset);
    run_test("search: returns -1 for missing key",   test_search_missing_key_returns_neg1);
    run_test("search: returns -1 on empty tree",     test_search_empty_tree_returns_neg1);
    run_test("search: returns -1 on NULL tree",      test_search_null_tree_returns_neg1);

    /* 1. 단위 테스트 — range */
    run_test("range: returns 0 on empty tree",       test_range_empty_tree_returns_zero);
    run_test("range: returns 0 on NULL tree",        test_range_null_tree_returns_zero);
    run_test("range: returns 0 when from > to",      test_range_from_gt_to_returns_zero);
    run_test("range: correct count for 5 keys",      test_range_basic_count);

    /* 2. 기능 테스트 — range 정확성 */
    run_test("range: boundary endpoints included",   test_range_boundary_inclusive);
    run_test("range: keys outside bounds excluded",  test_range_excludes_outside_keys);
    run_test("range: [x,x] returns exactly 1 key",  test_range_single_key);
    run_test("range: no results outside all keys",   test_range_no_results);

    /* 2. 기능 테스트 — 중복 키 */
    run_test("dup key: range returns all offsets",   test_duplicate_key_all_offsets_in_range);
    run_test("dup key: search returns valid offset", test_duplicate_key_search_returns_valid);

    /* 2. 기능 테스트 — 대량 삽입 / 분할 정합성 */
    run_test("bulk: sequential insert all searchable", test_sequential_insert_all_searchable);
    run_test("bulk: reverse insert all searchable",    test_reverse_insert_all_searchable);
    run_test("split: height increases after splits",   test_height_increases_on_split);
    run_test("split: small order 50-key bulk insert",  test_large_bulk_insert_small_order);

    /* 3. 엣지 케이스 — range_alloc */
    run_test("range_alloc: correct count",           test_range_alloc_correct_count);
    run_test("range_alloc: NULL tree returns NULL",  test_range_alloc_null_tree_returns_null);

    /* 3. 엣지 케이스 — IO 카운터 */
    run_test("io: last_io tracks node visits",       test_last_io_tracks_node_visits);

    /* 3. 엣지 케이스 — 경계 */
    run_test("range: respects max_count limit",      test_range_respects_max_count);
    run_test("range: to boundary is not exceeded",   test_range_to_is_exclusive_beyond);

    if (failures > 0) {
        fprintf(stderr, "\n%d/%d bptree tests failed.\n", failures, tests_run);
        return 1;
    }

    printf("\nAll %d bptree tests passed.\n", tests_run);
    return 0;
}
