#include "cache.h"
#include "work_with_disk.h"

int dbsync(const struct DB *db){
    struct MyDB *myDB = (struct MyDB *) db;
    struct CacheItem *item = myDB->cache->head;
    while(item != NULL){
        if(item->need_sync)
            node_flush(myDB, item->node);
        item = item->next;
    }
    return 0;
}
