#define __MYDB__WORK_WITH_KEY__
#ifndef __MYDB_DEF__
    #define __MYDB_DEF__
    #include "db.h"
#endif

void copy_key(struct BTreeNode *x, long i, struct BTreeNode *y, long j );

int compare(const struct DBT k1, const struct DBT k2);
