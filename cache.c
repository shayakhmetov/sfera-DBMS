#include "cache.h"
#include "node_alloc.h"

struct Hash *cache_find_helper(struct Cache *cache, int offset);

struct Cache *cache_create(size_t cache_size){
    struct Cache *cache = malloc(sizeof(struct Cache));
    cache->lru = malloc(sizeof(struct ListLRU));
    cache->cache_size = cache_size;
    cache->hash = NULL;
    TAILQ_INIT(cache->lru);
    cache->current_size = 0;
    return cache;
}

struct BTreeNode *cache_find_node(struct MyDB *myDB, int offset){
    struct Cache *cache = myDB->cache;
    struct Hash *h_item = cache_find_helper(cache, offset);
    if(h_item != NULL)
        return node_copy(myDB, h_item->node);
    else return NULL;
}

void cache_write_node(struct MyDB *myDB, struct BTreeNode *node2){
    struct Cache *cache = myDB->cache;
    struct Hash *h_item = cache_find_helper(cache, node2->offset);
    
    if(h_item != NULL){
        TAILQ_REMOVE(cache->lru, h_item->list_item, tailq);
        node_free(myDB, h_item->node);
        h_item->node = node_copy(myDB, node2);
        TAILQ_INSERT_HEAD(cache->lru, h_item->list_item, tailq);
    }
    else {
        cache_add_node(myDB, node2);
    }
}

void cache_add_node(struct MyDB *myDB, struct BTreeNode *node2){
    struct Cache *cache = myDB->cache;
    struct BTreeNode *node = node_copy(myDB, node2);
    struct Hash *h_item;
    
    if(cache->current_size == cache->cache_size){
        struct ListLRU_item *lru_last = TAILQ_LAST(cache->lru, ListLRU);
        cache_delete_node(myDB, lru_last->offset);
    }
    struct ListLRU_item *lru_item = malloc(sizeof(struct ListLRU_item));
    lru_item->offset = node->offset;
    TAILQ_INSERT_HEAD(cache->lru, lru_item, tailq);
    
    h_item = (struct Hash *) malloc(sizeof(struct Hash));
    h_item->offset = node->offset;
    h_item->node = node;
    h_item->list_item = lru_item;
    HASH_ADD_INT( cache->hash, offset, h_item);
    
    if(cache->current_size < cache->cache_size) cache->current_size++;
}

struct Hash *cache_find_helper(struct Cache *cache, int offset){
    struct Hash *h_item;
    if(cache->current_size == 0) return NULL;
    HASH_FIND_INT(cache->hash, &offset , h_item);  
    return h_item;
}

void cache_delete_node(struct MyDB *myDB, int offset){
    struct Cache *cache = myDB->cache;
    if(cache->current_size == 0) return;
    struct Hash *h_item = cache_find_helper(cache, offset);
    if(h_item != NULL){
        TAILQ_REMOVE(cache->lru, h_item->list_item, tailq);
        free(h_item->list_item);
        cache_hitem_free(myDB, h_item);
        cache->current_size--;
    }
}

void cache_hitem_free(struct MyDB *myDB, struct Hash *h_item){
    struct Cache *cache = myDB->cache;
    node_free(myDB, h_item->node);
    HASH_DEL(cache->hash, h_item);
    free(h_item);
}

void cache_free(struct MyDB *myDB){
    struct Cache *cache = myDB->cache;
    struct Hash *h_item, *tmp;
    
    struct ListLRU_item *lru_item; 
    while((lru_item = TAILQ_FIRST(cache->lru)) != NULL) {
        TAILQ_REMOVE(cache->lru, lru_item, tailq);
        free(lru_item);
    }
    HASH_ITER(hh, cache->hash, h_item, tmp) {
        cache_hitem_free(myDB, h_item);
    }
    
    free(cache->hash);
    free(cache);
}


