#define __MYDB__PRINT_FOR_DEBUG__
#ifndef __MYDB_DEF__
    #define __MYDB_DEF__
    #include "db.h"
#endif

int get_current_n_r(const struct MyDB *myDB, const struct BTreeNode * x); 
int get_current_n(const struct DB *db);

void print_DB_info(const struct DB *db);

void print_Node_info(const struct DB *db, struct BTreeNode *node);
