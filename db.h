#include <stdlib.h>
#define true 1
#define false 0

typedef char bool;

/* check `man dbopen` */
struct DBT{
     void  *data;
     size_t size;
}; //key || value

struct DB{
    /* Public API */
    int (*close)(const struct DB *db);
    int (*del)(struct DB *db, const struct DBT *key);
    int (*get)(const struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*sync)(const struct DB *db);
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DBC{
        /* Maximum on-disk file size */
        /* 512MB by default */
        size_t db_size;
        /* Maximum chunk (node/data chunk) size */
        /* 4KB by default */
        size_t chunk_size; //page,block 4KB
        /* Maximum memory size */
        /* 16MB by default */
        size_t mem_size; //ne nado 
};

struct DB *dbcreate(const char *file, const struct DBC *conf);
struct DB *dbopen  (const char *file); /* Metadata in file */

struct BTreeNode{
    bool leaf; 
    struct DBT *keys;
    struct DBT *values; 
    size_t *childs; // offsets
    size_t n; //current number of keys
    size_t offset; // position of node in file according to root
};

struct MyDB{
    /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, const struct DBT *key);
    int (*get)(const struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, const struct DBT *data);
    int (*sync)(const struct DB *db);
    
    size_t t;//const for max, max_node_keys = 2t-1
    size_t chunk_size;
    
    struct BTreeNode *root; // !!!not in file->DB_METADATA !!! 
    char *buffer; // not in file
    int id_file; // file handler, !!!not in file->DB_METADATA !!!
    
    bool *exist; //bitmask for existing pages in file
    size_t size; //of pages in BTree
    size_t max_size; //max pages .NOT db_size
};

//For debug purposes and more
struct BTreeSearchResult{
    struct BTreeNode *node;
    size_t index;
};
