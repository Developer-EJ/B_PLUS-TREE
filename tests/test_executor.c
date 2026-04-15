#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#  include <direct.h>
#  include <io.h>
#  define MKDIR(p) _mkdir(p)
#  define DUP _dup
#  define DUP2 _dup2
#  define CLOSE _close
#  define FILENO _fileno
#else
#  include <sys/stat.h>
#  include <unistd.h>
#  define MKDIR(p) mkdir(p, 0755)
#  define DUP dup
#  define DUP2 dup2
#  define CLOSE close
#  define FILENO fileno
int fileno(FILE *stream);
#endif

#include "../include/interface.h"
#include "../include/index_manager.h"

#define TEST_TABLE "executor_test_users"

typedef struct {
    ResultSet *result;
    char      *stdout_text;
    char      *stderr_text;
} CapturedSelect;

static int g_failures = 0;

static void copy_text(char *dst, size_t dst_size, const char *src) {
    if (!dst || dst_size == 0) return;
    if (!src) {
        dst[0] = '\0';
        return;
    }

    size_t len = strlen(src);
    if (len >= dst_size) len = dst_size - 1;
    memcpy(dst, src, len);
    dst[len] = '\0';
}

static char *dup_text(const char *src) {
    if (!src) src = "";

    size_t len = strlen(src) + 1;
    char *copy = (char *)malloc(len);
    if (!copy) return NULL;

    memcpy(copy, src, len);
    return copy;
}

static void report_failure(const char *test_name, const char *message) {
    fprintf(stderr, "[FAIL] %s: %s\n", test_name, message);
    g_failures++;
}

static int text_contains(const char *haystack, const char *needle) {
    return haystack && needle && strstr(haystack, needle) != NULL;
}

static char *read_text_file(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return dup_text("");

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return dup_text("");
    }

    long size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return dup_text("");
    }

    if (fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return dup_text("");
    }

    char *buf = (char *)calloc((size_t)size + 1, sizeof(char));
    if (!buf) {
        fclose(fp);
        return NULL;
    }

    size_t read_size = fread(buf, 1, (size_t)size, fp);
    fclose(fp);
    buf[read_size] = '\0';
    return buf;
}

static void build_data_path(char *buf, size_t size) {
    snprintf(buf, size, "data/%s.dat", TEST_TABLE);
}

static void build_schema_path(char *buf, size_t size) {
    snprintf(buf, size, "schema/%s.schema", TEST_TABLE);
}

static void build_capture_path(char *buf, size_t size, const char *suffix) {
    snprintf(buf, size, "logs/test_executor_%s.log", suffix);
}

static void remove_if_exists(const char *path) {
    if (!path) return;
    remove(path);
}

static int write_schema_file(void) {
    char schema_path[256];
    build_schema_path(schema_path, sizeof(schema_path));

    FILE *fp = fopen(schema_path, "wb");
    if (!fp) return 0;

    fprintf(fp,
            "table=%s\n"
            "columns=4\n"
            "col0=id,INT,0\n"
            "col1=name,VARCHAR,64\n"
            "col2=age,INT,0\n"
            "col3=email,VARCHAR,128\n",
            TEST_TABLE);
    fclose(fp);
    return 1;
}

static void reset_fixture(void) {
    char data_path[256];
    char schema_path[256];

    build_data_path(data_path, sizeof(data_path));
    build_schema_path(schema_path, sizeof(schema_path));

    index_cleanup();
    remove_if_exists(data_path);
    remove_if_exists(schema_path);
    write_schema_file();
}

static void cleanup_fixture(void) {
    char data_path[256];
    char schema_path[256];

    build_data_path(data_path, sizeof(data_path));
    build_schema_path(schema_path, sizeof(schema_path));

    index_cleanup();
    remove_if_exists(data_path);
    remove_if_exists(schema_path);
}

static void init_insert_stmt(InsertStmt *stmt, char **values, int value_count) {
    memset(stmt, 0, sizeof(*stmt));
    copy_text(stmt->table, sizeof(stmt->table), TEST_TABLE);
    stmt->values = values;
    stmt->value_count = value_count;
}

static void init_select_eq(SelectStmt *stmt, const char *col, const char *value) {
    memset(stmt, 0, sizeof(*stmt));
    stmt->select_all = 1;
    copy_text(stmt->table, sizeof(stmt->table), TEST_TABLE);
    stmt->has_where = 1;
    copy_text(stmt->where.col, sizeof(stmt->where.col), col);
    stmt->where.type = WHERE_EQ;
    copy_text(stmt->where.val, sizeof(stmt->where.val), value);
}

static void init_select_between(SelectStmt *stmt, const char *col,
                                const char *from, const char *to) {
    memset(stmt, 0, sizeof(*stmt));
    stmt->select_all = 1;
    copy_text(stmt->table, sizeof(stmt->table), TEST_TABLE);
    stmt->has_where = 1;
    copy_text(stmt->where.col, sizeof(stmt->where.col), col);
    stmt->where.type = WHERE_BETWEEN;
    copy_text(stmt->where.val_from, sizeof(stmt->where.val_from), from);
    copy_text(stmt->where.val_to, sizeof(stmt->where.val_to), to);
}

static int validate_insert_stmt(const InsertStmt *stmt, const TableSchema *schema) {
    ASTNode node;
    memset(&node, 0, sizeof(node));
    node.type = STMT_INSERT;
    node.insert = *stmt;
    return schema_validate(&node, schema);
}

static int validate_select_stmt(const SelectStmt *stmt, const TableSchema *schema) {
    ASTNode node;
    memset(&node, 0, sizeof(node));
    node.type = STMT_SELECT;
    node.select = *stmt;
    return schema_validate(&node, schema);
}

static int insert_row(const TableSchema *schema, char **values, int value_count) {
    InsertStmt stmt;
    init_insert_stmt(&stmt, values, value_count);

    if (validate_insert_stmt(&stmt, schema) != SQL_OK)
        return SQL_ERR;

    return db_insert(&stmt, schema);
}

static int capture_select(const SelectStmt *stmt, const TableSchema *schema,
                          const char *tag, CapturedSelect *captured) {
    char stdout_path[256];
    char stderr_path[256];
    FILE *stdout_file = NULL;
    FILE *stderr_file = NULL;
    int saved_stdout = -1;
    int saved_stderr = -1;
    int ok = 0;

    memset(captured, 0, sizeof(*captured));
    MKDIR("logs");

    build_capture_path(stdout_path, sizeof(stdout_path), tag);
    build_capture_path(stderr_path, sizeof(stderr_path), "stderr");
    if (strcmp(tag, "stderr") == 0) {
        build_capture_path(stderr_path, sizeof(stderr_path), "stderr_alt");
    }

    stdout_file = fopen(stdout_path, "w+b");
    stderr_file = fopen(stderr_path, "w+b");
    if (!stdout_file || !stderr_file) goto cleanup;

    fflush(stdout);
    fflush(stderr);

    saved_stdout = DUP(FILENO(stdout));
    saved_stderr = DUP(FILENO(stderr));
    if (saved_stdout < 0 || saved_stderr < 0) goto cleanup;

    if (DUP2(FILENO(stdout_file), FILENO(stdout)) < 0) goto cleanup;
    if (DUP2(FILENO(stderr_file), FILENO(stderr)) < 0) goto cleanup;

    captured->result = db_select(stmt, schema);

    fflush(stdout);
    fflush(stderr);

    if (DUP2(saved_stdout, FILENO(stdout)) < 0) goto cleanup;
    if (DUP2(saved_stderr, FILENO(stderr)) < 0) goto cleanup;

    CLOSE(saved_stdout);
    CLOSE(saved_stderr);
    saved_stdout = -1;
    saved_stderr = -1;

    fclose(stdout_file);
    fclose(stderr_file);
    stdout_file = NULL;
    stderr_file = NULL;

    captured->stdout_text = read_text_file(stdout_path);
    captured->stderr_text = read_text_file(stderr_path);
    ok = (captured->result != NULL &&
          captured->stdout_text != NULL &&
          captured->stderr_text != NULL);

cleanup:
    if (saved_stdout >= 0) {
        DUP2(saved_stdout, FILENO(stdout));
        CLOSE(saved_stdout);
    }
    if (saved_stderr >= 0) {
        DUP2(saved_stderr, FILENO(stderr));
        CLOSE(saved_stderr);
    }
    if (stdout_file) fclose(stdout_file);
    if (stderr_file) fclose(stderr_file);

    remove_if_exists(stdout_path);
    remove_if_exists(stderr_path);
    return ok;
}

static void free_captured_select(CapturedSelect *captured) {
    if (!captured) return;
    result_free(captured->result);
    free(captured->stdout_text);
    free(captured->stderr_text);
}

static void test_linear_path_returns_row_and_logs(void) {
    const char *test_name = "linear_path_returns_row_and_logs";
    TableSchema *schema = NULL;
    CapturedSelect hit = {0};
    CapturedSelect miss = {0};
    int ok = 1;

    reset_fixture();
    schema = schema_load(TEST_TABLE);
    if (!schema) {
        report_failure(test_name, "schema_load failed");
        goto cleanup;
    }

    if (index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0) {
        report_failure(test_name, "index_init failed");
        goto cleanup;
    }

    {
        char id[] = "1";
        char name[] = "alice";
        char age[] = "25";
        char email[] = "alice@example.com";
        char *values[] = { id, name, age, email };

        if (insert_row(schema, values, 4) != SQL_OK) {
            report_failure(test_name, "db_insert failed");
            goto cleanup;
        }
    }

    {
        SelectStmt stmt;
        init_select_eq(&stmt, "name", "alice");
        if (validate_select_stmt(&stmt, schema) != SQL_OK) {
            report_failure(test_name, "schema_validate failed for name hit");
            goto cleanup;
        }

        if (!capture_select(&stmt, schema, "stdout", &hit)) {
            report_failure(test_name, "capture_select failed for name hit");
            goto cleanup;
        }

        if (!hit.result || hit.result->row_count != 1)
            ok = 0;
        if (!hit.result || strcmp(hit.result->rows[0].values[1], "alice") != 0)
            ok = 0;
        if (!hit.stdout_text || hit.stdout_text[0] != '\0')
            ok = 0;
        if (!text_contains(hit.stderr_text, "[SELECT][linear"))
            ok = 0;
        if (!text_contains(hit.stderr_text, "tree_h(id)=") ||
            !text_contains(hit.stderr_text, "tree_h(age)="))
            ok = 0;
    }

    {
        SelectStmt stmt;
        init_select_eq(&stmt, "name", "nobody");
        if (validate_select_stmt(&stmt, schema) != SQL_OK) {
            report_failure(test_name, "schema_validate failed for name miss");
            goto cleanup;
        }

        if (!capture_select(&stmt, schema, "stdout_miss", &miss)) {
            report_failure(test_name, "capture_select failed for name miss");
            goto cleanup;
        }

        if (!miss.result || miss.result->row_count != 0)
            ok = 0;
        if (!miss.stdout_text || miss.stdout_text[0] != '\0')
            ok = 0;
        if (!text_contains(miss.stderr_text, "[SELECT][linear"))
            ok = 0;
    }

    if (!ok)
        report_failure(test_name, "linear path assertions failed");

cleanup:
    free_captured_select(&hit);
    free_captured_select(&miss);
    schema_free(schema);
    cleanup_fixture();
}

static void test_index_labels_are_emitted(void) {
    const char *test_name = "index_labels_are_emitted";
    TableSchema *schema = NULL;
    CapturedSelect id_eq = {0};
    CapturedSelect id_range = {0};
    CapturedSelect age_range = {0};
    int ok = 1;

    reset_fixture();
    schema = schema_load(TEST_TABLE);
    if (!schema) {
        report_failure(test_name, "schema_load failed");
        goto cleanup;
    }

    if (index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0) {
        report_failure(test_name, "index_init failed");
        goto cleanup;
    }

    {
        char id1[] = "1";
        char name1[] = "alice";
        char age1[] = "21";
        char email1[] = "alice@example.com";
        char *row1[] = { id1, name1, age1, email1 };

        char id2[] = "2";
        char name2[] = "bob";
        char age2[] = "32";
        char email2[] = "bob@example.com";
        char *row2[] = { id2, name2, age2, email2 };

        char id3[] = "3";
        char name3[] = "carol";
        char age3[] = "43";
        char email3[] = "carol@example.com";
        char *row3[] = { id3, name3, age3, email3 };

        if (insert_row(schema, row1, 4) != SQL_OK ||
            insert_row(schema, row2, 4) != SQL_OK ||
            insert_row(schema, row3, 4) != SQL_OK) {
            report_failure(test_name, "db_insert failed");
            goto cleanup;
        }
    }

    {
        SelectStmt stmt;
        init_select_eq(&stmt, "id", "2");
        if (validate_select_stmt(&stmt, schema) != SQL_OK ||
            !capture_select(&stmt, schema, "id_eq", &id_eq)) {
            report_failure(test_name, "id eq select failed");
            goto cleanup;
        }
        if (!text_contains(id_eq.stderr_text, "index:id:eq"))
            ok = 0;
        if (!id_eq.stdout_text || id_eq.stdout_text[0] != '\0')
            ok = 0;
    }

    {
        SelectStmt stmt;
        init_select_between(&stmt, "id", "1", "3");
        if (validate_select_stmt(&stmt, schema) != SQL_OK ||
            !capture_select(&stmt, schema, "id_range", &id_range)) {
            report_failure(test_name, "id range select failed");
            goto cleanup;
        }
        if (!text_contains(id_range.stderr_text, "index:id:range"))
            ok = 0;
        if (!id_range.stdout_text || id_range.stdout_text[0] != '\0')
            ok = 0;
    }

    {
        SelectStmt stmt;
        init_select_between(&stmt, "age", "20", "40");
        if (validate_select_stmt(&stmt, schema) != SQL_OK ||
            !capture_select(&stmt, schema, "age_range", &age_range)) {
            report_failure(test_name, "age range select failed");
            goto cleanup;
        }
        if (!text_contains(age_range.stderr_text, "index:age:range"))
            ok = 0;
        if (!age_range.stdout_text || age_range.stdout_text[0] != '\0')
            ok = 0;
    }

    if (!ok)
        report_failure(test_name, "index label assertions failed");

cleanup:
    free_captured_select(&id_eq);
    free_captured_select(&id_range);
    free_captured_select(&age_range);
    schema_free(schema);
    cleanup_fixture();
}

static void test_repeated_index_lifecycle_is_safe(void) {
    const char *test_name = "repeated_index_lifecycle_is_safe";
    TableSchema *schema = NULL;
    int ok = 1;

    reset_fixture();
    schema = schema_load(TEST_TABLE);
    if (!schema) {
        report_failure(test_name, "schema_load failed");
        goto cleanup;
    }

    if (index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0) {
        report_failure(test_name, "initial index_init failed");
        goto cleanup;
    }

    {
        char id[] = "11";
        char name[] = "repeat";
        char age[] = "55";
        char email[] = "repeat@example.com";
        char *values[] = { id, name, age, email };

        if (insert_row(schema, values, 4) != SQL_OK) {
            report_failure(test_name, "seed insert failed");
            goto cleanup;
        }
    }

    index_cleanup();

    for (int i = 0; i < 3; i++) {
        CapturedSelect captured = {0};
        SelectStmt stmt;

        if (index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0)
            ok = 0;
        if (index_init(TEST_TABLE, IDX_ORDER_DEFAULT, IDX_ORDER_DEFAULT) != 0)
            ok = 0;

        init_select_eq(&stmt, "name", "repeat");
        if (validate_select_stmt(&stmt, schema) != SQL_OK ||
            !capture_select(&stmt, schema, "lifecycle", &captured)) {
            free_captured_select(&captured);
            report_failure(test_name, "select in lifecycle loop failed");
            goto cleanup;
        }

        if (!captured.result || captured.result->row_count != 1)
            ok = 0;
        if (!text_contains(captured.stderr_text, "[SELECT][linear"))
            ok = 0;

        free_captured_select(&captured);
        index_cleanup();
    }

    if (!ok)
        report_failure(test_name, "lifecycle assertions failed");

cleanup:
    schema_free(schema);
    cleanup_fixture();
}

int main(void) {
    test_linear_path_returns_row_and_logs();
    test_index_labels_are_emitted();
    test_repeated_index_lifecycle_is_safe();

    if (g_failures == 0) {
        printf("test_executor: PASS\n");
        return 0;
    }

    printf("test_executor: FAIL (%d)\n", g_failures);
    return 1;
}
