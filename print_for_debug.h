#ifndef __MYDB__PRINT_FOR_DEBUG__H__
#define __MYDB__PRINT_FOR_DEBUG__H__
#include "db.h"

int get_current_n_r(struct MyDB *myDB, struct BTreeNode * x); 
int get_current_n(struct DB *db);

void print_DB_info(struct DB *db);

void print_Node_info(struct DB *db, struct BTreeNode *node);

#endif
