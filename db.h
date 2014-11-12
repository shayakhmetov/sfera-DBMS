#ifndef __MYDB_DEF__H__
#define __MYDB_DEF__H__

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include "uthash.h"
#include <sys/queue.h>

#define MAX_KEY_LENGTH  22
#define MAX_VALUE_LENGTH  22

#define true 1
#define false 0

typedef char bool;
typedef unsigned char byte;

struct DBT{
     void  *data;
     size_t size;
}; //key || value

struct DB{
    /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*sync)(struct DB *db);
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
        size_t mem_size;
};


int db_close(struct DB *db);
int db_del(struct DB *, void *, size_t);
int db_get(struct DB *, void *, size_t, void **, size_t *);
int db_put(struct DB *, void *, size_t, void * , size_t  );

struct BTreeNode{
    bool leaf; 
    struct DBT *keys;
    struct DBT *values; 
    size_t *childs; // offsets
    size_t n; //current number of keys
    size_t offset; // position of node in file according to root
};

struct Cache {
    size_t cache_size;
    size_t current_size;
    struct ListLRU *lru;
    struct Hash *hash;
};

struct ListLRU_item {
    TAILQ_ENTRY(ListLRU_item) tailq;
    size_t offset;
};

TAILQ_HEAD(ListLRU, ListLRU_item);

struct Hash {
    int offset;
    struct BTreeNode *node;
    struct ListLRU_item *list_item;
    UT_hash_handle hh;
};

struct MyDB {
    /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*sync)(struct DB *db);
    
    size_t t;//const for max, max_node_keys = 2t-1
    size_t chunk_size;
    
    struct BTreeNode *root; // !!!not in file->DB_METADATA !!! 
    byte *buffer; // not in file
    int id_file; // file handler, !!!not in file->DB_METADATA !!!
    
    byte *exist; //bitset for existing pages in file
    size_t size; //of pages in BTree
    size_t max_size; //max_size == max pages (.NOT db_size)
    int depth; //without delete!!
    size_t cache_size; //in cache_elements
    
    struct Cache *cache;
};

//For debug purposes and more
struct BTreeSearchResult{
    struct BTreeNode *node;
    size_t index;
};

#endif
