#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/interface.h"
#include "../../include/index_manager.h"
#include "../../include/bptree.h"

typedef struct {
    char    table[64];
    BPTree *tree_id;
    BPTree *tree_age;
    int     initialized;
    int     last_io_id;
    int     last_io_age;
} TableIndex;

static TableIndex g_tables[IDX_MAX_TABLES];
static int        g_count = 0;

static TableIndex *find_entry(const char *table) {
    for (int i = 0; i < g_count; i++) {
        if (strcmp(g_tables[i].table, table) == 0)
            return &g_tables[i];
    }
    return NULL;
}

static int col_value(const char *line, int n, char *buf, int buf_size) {
    const char *p = line;
    int col = 0;

    while (*p && col < n) {
        if (*p == '|') col++;
        p++;
    }
    if (col < n) return 0;

    while (*p == ' ') p++;

    int i = 0;
    while (*p && *p != '|' && *p != '\n' && *p != '\r' && i < buf_size - 1)
        buf[i++] = *p++;

    while (i > 0 && buf[i - 1] == ' ') i--;
    buf[i] = '\0';
    return 1;
}

int index_init(const char *table, int order_id, int order_age) {
    if (!table || g_count >= IDX_MAX_TABLES) return -1;

    if (find_entry(table)) return 0;

    TableIndex *ti = &g_tables[g_count];
    memset(ti, 0, sizeof(*ti));
    strncpy(ti->table, table, sizeof(ti->table) - 1);

    int oid = (order_id > 2) ? order_id : IDX_ORDER_DEFAULT;
    int oage = (order_age > 2) ? order_age : IDX_ORDER_DEFAULT;

    ti->tree_id = bptree_create(oid);
    ti->tree_age = bptree_create(oage);
    if (!ti->tree_id || !ti->tree_age) {
        bptree_destroy(ti->tree_id);
        bptree_destroy(ti->tree_age);
        return -1;
    }

    g_count++;

    char path[256];
    snprintf(path, sizeof(path), "data/%s.dat", table);

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "[index] '%s' initialized (file missing, empty index)\n",
                table);
        ti->initialized = 1;
        return 0;
    }

    char line[1024];
    char col_buf[64];
    int inserted = 0;

    while (1) {
        long offset = ftell(fp);
        if (!fgets(line, sizeof(line), fp)) break;

        int len = (int)strlen(line);
        while (len > 0 &&
               (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0) continue;

        if (!col_value(line, 0, col_buf, sizeof(col_buf))) continue;
        int id = atoi(col_buf);

        if (!col_value(line, 2, col_buf, sizeof(col_buf))) continue;
        int age = atoi(col_buf);

        bptree_insert(ti->tree_id, id, offset);
        bptree_insert(ti->tree_age, age, offset);
        inserted++;
    }

    fclose(fp);

    fprintf(stderr,
            "[index] '%s' initialized - %d rows loaded "
            "(order_id=%d, order_age=%d)\n",
            table, inserted, oid, oage);

    ti->initialized = 1;
    return 0;
}

void index_cleanup(void) {
    for (int i = 0; i < g_count; i++) {
        bptree_destroy(g_tables[i].tree_id);
        bptree_destroy(g_tables[i].tree_age);
        memset(&g_tables[i], 0, sizeof(g_tables[i]));
    }
    g_count = 0;
}

int index_insert_id(const char *table, int id, long offset) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;
    return bptree_insert(ti->tree_id, id, offset);
}

long index_search_id(const char *table, int id) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;

    long offset = bptree_search(ti->tree_id, id);
    ti->last_io_id = bptree_last_io(ti->tree_id);
    return offset;
}

long *index_range_id_alloc(const char *table, int from, int to,
                           int *out_count) {
    TableIndex *ti = find_entry(table);
    if (out_count) *out_count = 0;
    if (!ti || !out_count) return NULL;

    long *offsets = bptree_range_alloc(ti->tree_id, from, to, out_count);
    ti->last_io_id = bptree_last_io(ti->tree_id);
    return offsets;
}

int index_range_id(const char *table, int from, int to,
                   long *offsets, int max) {
    if (!offsets || max <= 0) return 0;

    int count = 0;
    long *all_offsets = index_range_id_alloc(table, from, to, &count);
    if (!all_offsets || count <= 0) return 0;

    int copied = (count < max) ? count : max;
    memcpy(offsets, all_offsets, (size_t)copied * sizeof(long));
    free(all_offsets);
    return copied;
}

int index_insert_age(const char *table, int age, long offset) {
    TableIndex *ti = find_entry(table);
    if (!ti) return -1;
    return bptree_insert(ti->tree_age, age, offset);
}

long *index_range_age_alloc(const char *table, int from, int to,
                            int *out_count) {
    TableIndex *ti = find_entry(table);
    if (out_count) *out_count = 0;
    if (!ti || !out_count) return NULL;

    long *offsets = bptree_range_alloc(ti->tree_age, from, to, out_count);
    ti->last_io_age = bptree_last_io(ti->tree_age);
    return offsets;
}

int index_range_age(const char *table, int from, int to,
                    long *offsets, int max) {
    if (!offsets || max <= 0) return 0;

    int count = 0;
    long *all_offsets = index_range_age_alloc(table, from, to, &count);
    if (!all_offsets || count <= 0) return 0;

    int copied = (count < max) ? count : max;
    memcpy(offsets, all_offsets, (size_t)copied * sizeof(long));
    free(all_offsets);
    return copied;
}

int index_height_id(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return bptree_height(ti->tree_id);
}

int index_height_age(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return bptree_height(ti->tree_age);
}

void index_reset_io_stats(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return;

    ti->last_io_id = 0;
    ti->last_io_age = 0;
}

int index_last_io_id(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return ti->last_io_id;
}

int index_last_io_age(const char *table) {
    TableIndex *ti = find_entry(table);
    if (!ti) return 0;
    return ti->last_io_age;
}
