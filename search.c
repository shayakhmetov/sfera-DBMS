#include "search.h"

#include "work_with_disk.h"
#include "work_with_key.h"
#include "node_alloc.h"

struct BTreeSearchResult *search_recursive(const struct MyDB *myDB, struct BTreeNode *x, struct DBT *key){
    long i;
    for(i=0; i<x->n && compare(*key, x->keys[i]) > 0; i++) ;
    if( i< x->n && compare(*key, x->keys[i]) == 0){
        struct BTreeSearchResult *res = malloc(sizeof(struct BTreeSearchResult));
        res->node = x;
        res->index = i;
        return res;
    }
    else if(x->leaf){
        return NULL;
    }
    else{
        struct BTreeNode *c = node_disk_read(myDB, x->childs[i]);
        struct BTreeSearchResult *res = search_recursive(myDB, c , key);
        if(res!=NULL && res->node == c) return res;   
        node_free(myDB,c);
        return res;
    }
}


int search(const struct DB *db, struct DBT *key, struct DBT *value){
    const struct MyDB *myDB = (const struct MyDB *)db;
    if(myDB->root->n==0) return -1;
    struct BTreeSearchResult *res = search_recursive(myDB, myDB->root, key);
    if(res == NULL) return -1;
    value->data = malloc(res->node->values[res->index].size);
    memcpy(value->data, res->node->values[res->index].data, res->node->values[res->index].size);
    value->size = res->node->values[res->index].size;
    if(res->node != myDB->root && res->node!=NULL) node_free(myDB,res->node);
    free(res); 
    return 0;
}

