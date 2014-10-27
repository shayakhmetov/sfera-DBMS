#define __MYDB__WORK_WITH_DISK__
#ifndef __MYDB_DEF__
    #define __MYDB_DEF__
    #include "db.h"
#endif

size_t convert_offset(const struct MyDB *myDB, size_t offset);

int node_disk_write(struct MyDB *myDB, struct BTreeNode *node);

struct BTreeNode *node_disk_read(const struct MyDB *myDB, size_t offset );
    
int dbmetadata_disk_write(struct MyDB *myDB, long index );

int node_disk_delete(struct MyDB *myDB, size_t offset);

int dbclose(struct DB *db);

struct DB *dbcreate(const char *file, struct DBC conf);

struct DB *dbopen(const char *file);
