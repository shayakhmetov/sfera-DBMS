#define __MYDB__NODE_ALLOC__
#ifndef __MYDB_DEF__
    #define __MYDB_DEF__
    #include "db.h"
#endif

struct BTreeNode *node_malloc(const struct MyDB *myDB);

void node_free(const struct MyDB *myDB, struct BTreeNode *s);

int assign_BTreeNode(struct MyDB *myDB, struct BTreeNode *node);

struct BTreeNode *create_BTreeNode(struct MyDB *myDB);
