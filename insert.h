#ifndef __MYDB_INSERT__H__
#define __MYDB_INSERT__H__
#include "db.h"



struct BTreeNode *split_child(struct MyDB *myDB, struct BTreeNode *x, long i, struct BTreeNode *y);

int insert_nonfull(struct MyDB *myDB, struct BTreeNode *x, struct DBT *key, struct DBT *data);

int insert(struct DB *db, struct DBT *key, struct DBT *data);

#endif
