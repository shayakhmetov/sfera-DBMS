#define __MYDB__SEARCH__
#ifndef __MYDB_DEF__
    #define __MYDB_DEF__
    #include "db.h"
#endif

struct BTreeSearchResult *search_recursive(const struct MyDB *myDB, struct BTreeNode *x, struct DBT *key);

int search(const struct DB *db, struct DBT *key, struct DBT *value);

