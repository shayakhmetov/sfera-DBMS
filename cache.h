#ifndef __MYDB__CACHE__H
#define __MYDB__CACHE__H
#include "db.h"

int dbsync(const struct DB *db);

void cache_create(struct MyDB *myDB);
struct BTreeNode * cache_find_node(const struct MyDB *myDB, size_t offset);
bool cache_write_node(struct MyDB *myDB, struct BTreeNode *node); //false if not found, true if found and changed
bool cache_delete_node(struct MyDB *myDB, struct BTreeNode *node); //false if not found, true if found and changed
void cache_free(struct MyDB *myDB);

#endif
