#ifndef EXECUTOR_INTERNAL_H
#define EXECUTOR_INTERNAL_H

#include "../../include/interface.h"

typedef struct {
    char   path[32];
    double elapsed_ms;
    int    tree_io;
    int    row_count;
} SelectExecInfo;

ResultSet *db_select_mode(const SelectStmt *stmt, const TableSchema *schema,
                          int force_linear, int emit_log,
                          SelectExecInfo *info);

#endif /* EXECUTOR_INTERNAL_H */
