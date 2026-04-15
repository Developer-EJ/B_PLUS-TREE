#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#define IDX_ORDER_DEFAULT  128
#define IDX_ORDER_SMALL      4
#define IDX_MAX_RANGE    65536
#define IDX_MAX_TABLES       8

int  index_init(const char *table, int order_id, int order_age);
void index_cleanup(void);

int  index_insert_id(const char *table, int id, long offset);
long index_search_id(const char *table, int id);
int  index_range_id(const char *table, int from, int to,
                    long *offsets, int max);
long *index_range_id_alloc(const char *table, int from, int to,
                           int *out_count);

int  index_insert_age(const char *table, int age, long offset);
int  index_range_age(const char *table, int from, int to,
                     long *offsets, int max);
long *index_range_age_alloc(const char *table, int from, int to,
                            int *out_count);

int  index_height_id(const char *table);
int  index_height_age(const char *table);

void index_reset_io_stats(const char *table);
int  index_last_io_id(const char *table);
int  index_last_io_age(const char *table);

#endif /* INDEX_MANAGER_H */
