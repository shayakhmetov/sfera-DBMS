#ifndef __MYDB__WORK_WITH_KEY__H__
#define __MYDB__WORK_WITH_KEY__H__
#include "db.h"

void copy_key(struct BTreeNode *x, long i, struct BTreeNode *y, long j );

int compare(const struct DBT k1, const struct DBT k2);

#endif
