#ifndef __MYDB__NODE_ALLOC__H__
#define __MYDB__NODE_ALLOC__H__
#include "db.h"
struct BTreeNode *node_malloc(const struct MyDB *myDB);

struct BTreeNode *node_copy(struct MyDB *myDB, struct BTreeNode *node);

void node_free(const struct MyDB *myDB, struct BTreeNode *s);

int assign_BTreeNode(struct MyDB *myDB, struct BTreeNode *node);

struct BTreeNode *create_BTreeNode(struct MyDB *myDB);

#endif
