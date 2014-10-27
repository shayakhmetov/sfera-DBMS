#define __MYDB__DELETE__
#ifndef __MYDB_DEF__
    #define __MYDB_DEF__
    #include "db.h"
#endif

void get_predecessor_key(struct MyDB *myDB, struct BTreeNode *x, struct DBT * key, struct DBT *k, struct DBT *v);

void get_successor_key(struct MyDB *myDB, struct BTreeNode *x, struct DBT * key, struct DBT *k, struct DBT *v);

void remove_key(struct BTreeNode *x, long i);

int merge_nodes(struct MyDB *myDB, struct BTreeNode *x, struct BTreeNode *a, struct BTreeNode *b, size_t index);

int delete_case3_helper(struct MyDB *myDB, struct BTreeNode *x,struct BTreeNode *a,struct BTreeNode *y, struct DBT *key, long i);

int delete_from_node(struct MyDB *myDB, struct BTreeNode *x, struct DBT * key);
    
int delete(struct DB *db, struct DBT * key);

