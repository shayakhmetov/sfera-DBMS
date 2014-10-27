#define __MYDB__INSERT__
#ifndef __MYDB_DEF__
    #define __MYDB_DEF__
    #include "db.h"
#endif


struct BTreeNode *split_child(struct MyDB *myDB, struct BTreeNode *x, long i, struct BTreeNode *y);

int insert_nonfull(struct MyDB *myDB, struct BTreeNode *x, struct DBT *key, struct DBT *data);

int insert(struct DB *db, struct DBT *key, struct DBT *data);
