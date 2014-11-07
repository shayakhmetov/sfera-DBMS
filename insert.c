#include "insert.h"

#include "work_with_disk.h"
#include "work_with_key.h"
#include "node_alloc.h"


struct BTreeNode *split_child(struct MyDB *myDB, struct BTreeNode *x, long i, struct BTreeNode *y){//returns new created node
    //y ~= x->childs[i]
    struct BTreeNode *z = create_BTreeNode(myDB);
    z->leaf = y->leaf;
    z->n = myDB->t - 1;
    long j;
    for(j=0; j < myDB->t - 1; j++){
        copy_key(z,j, y,j+myDB->t);
    }
    if(!y->leaf){
        for(j=0; j < myDB->t; j++ )
            z->childs[j] = y->childs[j + myDB->t];
    }
    y->n = myDB->t - 1;

    for(j=x->n; j>i; j--)
        x->childs[j+1]= x->childs[j];
    x->childs[i+1] = z->offset;
    
    for(j=x->n; j>i; j--){
        copy_key(x,j, x,j-1);
    }
    copy_key(x,i, y,myDB->t-1);
        
    x->n ++;
    
    node_disk_write(myDB,x);
    node_disk_write(myDB,y);
    node_disk_write(myDB,z);
    
    return z;
}


int insert_nonfull(struct MyDB *myDB, struct BTreeNode *x, struct DBT *key, struct DBT *data){
    if(x->leaf){
        if(x->n == 0){
            memcpy(x->keys[0].data, key->data, (key->size < MAX_KEY_LENGTH) ? key->size : MAX_KEY_LENGTH);
            x->keys[0].size = key->size;
            memcpy(x->values[0].data, data->data, (data->size < MAX_VALUE_LENGTH) ? data->size : MAX_VALUE_LENGTH);
            x->values[0].size = data->size;            
        }
        else{
            long i;
            for(i=0; i < x->n && compare(*key , x->keys[i]) > 0; i++ ) ;
            if( i < x->n  && compare(*key , x->keys[i]) == 0){
                memcpy(x->values[i].data, data->data, (data->size < MAX_VALUE_LENGTH) ? data->size : MAX_VALUE_LENGTH);
                x->values[i].size = data->size;
                node_disk_write(myDB,x);
                return 0; 
            }
            long j;
            for(j=x->n; j>i;  j-- ){
                copy_key(x,j, x,j-1);
            }
            memcpy(x->keys[i].data, key->data, (key->size < MAX_KEY_LENGTH) ? key->size : MAX_KEY_LENGTH);
            x->keys[i].size = key->size;
            memcpy(x->values[i].data,data->data, (data->size < MAX_VALUE_LENGTH) ? data->size : MAX_VALUE_LENGTH);
            x->values[i].size = data->size;
        }
        x->n ++;
        node_disk_write(myDB,x);
    }
    else{
        long i;
        for(i=0; i<x->n && compare(*key , x->keys[i])>0 ; i++) ;
        
        if(i<x->n && compare(*key , x->keys[i])==0  ){//if key already in x
            memcpy(x->values[i].data, data->data, (data->size < MAX_VALUE_LENGTH) ? data->size : MAX_VALUE_LENGTH);
            x->values[i].size = data->size;
            node_disk_write(myDB,x);
            return 0;
        }
        
        bool succ_child = false;        
        struct BTreeNode *c = node_disk_read(myDB, x->childs[i]); 
        struct BTreeNode *z = NULL;
        if(c->n == 2*(myDB->t)-1){
            z = split_child(myDB,x,i,c);
            if(compare(*key, x->keys[i])>0) {
                succ_child = true;
            }
        }
        if(succ_child == true) {
            insert_nonfull(myDB, z, key, data);
        }
        else{
            insert_nonfull(myDB, c, key, data);
        }
        
        node_free(myDB,c);
        if(z!=NULL) node_free(myDB,z);
    }
    return 0;
}

int insert(struct DB *db, struct DBT *key, struct DBT *data){
    struct MyDB *myDB = (struct MyDB *)db;
    struct BTreeNode *r = myDB->root;
    if(r->n == 2*(myDB->t)-1){
        struct BTreeNode *s = create_BTreeNode(myDB);
        s->leaf = false;
        myDB->depth++;
        s->childs[0] = r->offset;
        s->n = 0; //by default in node_malloc by create_BTreeNode
        
        dbmetadata_disk_write(myDB,-1); // update root offset, -1 is coz no need to update bitset
        
        myDB->root = s;
        struct BTreeNode *z = split_child(myDB,myDB->root, 0 ,r);
        
        node_free(myDB,r);
        node_free(myDB,z);
        
        insert_nonfull(myDB, myDB->root, key, data);
    }
    else {
        insert_nonfull(myDB, r, key ,data);
    }
    return 0;
}

