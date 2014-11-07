#ifndef __MYDB__PRINT_FOR_DEBUG__H__
#define __MYDB__PRINT_FOR_DEBUG__H__
#include "db.h"

int get_current_n_r(const struct MyDB *myDB, const struct BTreeNode * x); 
int get_current_n(const struct DB *db);

void print_DB_info(const struct DB *db);

void print_Node_info(const struct DB *db, struct BTreeNode *node);

#endif
