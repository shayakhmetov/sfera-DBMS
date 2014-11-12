#ifndef __MYDB__CACHE__H
#define __MYDB__CACHE__H
#include "db.h"
//int dbsync(const struct DB *db);

struct Cache *cache_create(size_t cache_size);

struct BTreeNode *cache_find_node(struct MyDB *myDB, int offset);

void cache_write_node(struct MyDB *myDB, struct BTreeNode *node2);//with find
void cache_add_node(struct MyDB *myDB, struct BTreeNode *node2);//without find

void cache_delete_node(struct MyDB *myDB, int offset);

void cache_hitem_free(struct MyDB *myDB, struct Hash *h_item);

void cache_free(struct MyDB *myDB);

#endif
