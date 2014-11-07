#ifndef __MYDB__SEARCH__H__
#define __MYDB__SEARCH__H__
#include "db.h"

struct BTreeSearchResult *search_recursive(const struct MyDB *myDB, struct BTreeNode *x, struct DBT *key);

int search(const struct DB *db, struct DBT *key, struct DBT *value);

#endif
