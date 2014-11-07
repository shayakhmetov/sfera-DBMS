#include "delete.h"

#include "work_with_disk.h"
#include "work_with_key.h"
#include "node_alloc.h"


void get_predecessor_key(struct MyDB *myDB, struct BTreeNode *x, struct DBT * key, struct DBT *k, struct DBT *v){
    if(x->leaf){
        k->size = x->keys[x->n-1].size;
        memcpy(k->data, x->keys[x->n-1].data, MAX_KEY_LENGTH);
        v->size = x->values[x->n-1].size;
        memcpy(v->data, x->values[x->n-1].data, MAX_VALUE_LENGTH);
    }
    else{
        struct BTreeNode *c = node_disk_read(myDB,x->childs[x->n]);
        get_predecessor_key(myDB,c,key, k, v);
        node_free(myDB,c);
    }
}

void get_successor_key(struct MyDB *myDB, struct BTreeNode *x, struct DBT * key, struct DBT *k, struct DBT *v){
    if(x->leaf){
        k->size = x->keys[0].size;
        memcpy(k->data, x->keys[0].data, MAX_KEY_LENGTH);
        v->size = x->values[0].size;
        memcpy(v->data, x->values[0].data, MAX_VALUE_LENGTH);
    }
    else{
        struct BTreeNode *c = node_disk_read(myDB,x->childs[0]);
        get_successor_key(myDB,c,key, k, v);
        node_free(myDB,c);
    }
}

void remove_key(struct BTreeNode *x, long i){ //remove x->keys[i] == Lshift to i
    long j;
    for(j=i;j<x->n-1;j++){
        copy_key(x,j, x,j+1);
    }
}

int merge_nodes(struct MyDB *myDB, struct BTreeNode *x, struct BTreeNode *a, struct BTreeNode *b, size_t index){ //index ~ key==x->keys[index]
    
    copy_key(a,a->n, x,index);
    
    long j;
    for(j=0; j<b->n; j++){
        copy_key(a,a->n+1+j, b,j);
    }
    for(j=0; j<b->n+1;j++)
        a->childs[a->n+1+j] = b->childs[j];  

    remove_key(x,index);
    for(j=index+1;j<x->n;j++)
        x->childs[j]=x->childs[j+1];
    x->n--;
    
    a->n += 1 + b->n;
    
    node_disk_delete(myDB,b->offset);
    
    return 0;//does not free memory, not disk_node_write
}

int delete_case3_helper(struct MyDB *myDB, struct BTreeNode *x,struct BTreeNode *a,struct BTreeNode *y, struct DBT *key, long i){
    struct BTreeNode *b = NULL;
    if(i<x->n){
          b = node_disk_read(myDB,x->childs[i+1]);
    }
    long j;
    int res = 0;
    if(i<x->n && b->n >= myDB->t){
        copy_key(y,y->n, x,i);
        
        copy_key(x,i, b,0);
        
        remove_key(b,0);
        
        y->childs[y->n+1] = b->childs[0];
        
        for(j=0;j<b->n;j++){
            b->childs[j] = b->childs[j+1];
        }
        
        b->n--;
        y->n++;
        node_disk_write(myDB, x);
        node_disk_write(myDB, b);
        node_disk_write(myDB, y);
        node_free(myDB,b);

        res = delete_from_node(myDB,y,key);
        node_free(myDB,y);
        return res;
    }
    else if(((i<x->n && b->n == myDB->t -1) || i==x->n) && ((i>0 && a->n == myDB->t -1) || i==0)){
        if(i>0){
            merge_nodes(myDB, x, a, y, i-1);
            node_free(myDB, y); 
            if(b!=NULL) node_free(myDB, b);
            node_disk_write(myDB, a);
            if(x==myDB->root && x->n==0){
                myDB->depth--;
                myDB->root = a;
                node_disk_delete(myDB,x->offset);
                dbmetadata_disk_write(myDB,-1);//update root offset
                node_free(myDB,x);
                res = delete_from_node(myDB,a,key);
                return res;
            }
            node_disk_write(myDB, x);
        
            res = delete_from_node(myDB,a,key);
            node_free(myDB,a);
            return res;
        }
        else {
            merge_nodes(myDB, x, y, b, i);
            node_disk_write(myDB, y);
            node_free(myDB, b);
            if(x==myDB->root && x->n==0){
                myDB->depth--;
                myDB->root = y;
                node_disk_delete(myDB,x->offset);
                dbmetadata_disk_write(myDB,-1);//update root offset
                node_free(myDB,x);
                int res = delete_from_node(myDB,y,key);
                return res;
            }
            node_disk_write(myDB, x);
            res = delete_from_node(myDB,y,key);
            
            node_free(myDB,y);
            return res; 
        }
    }
    else{
        return 0;
    }
}

int delete_from_node(struct MyDB *myDB, struct BTreeNode *x, struct DBT * key){
    long i;
    long j;
    int res = 0;
    for(i=0;i<x->n && compare(*key , x->keys[i]) > 0;i++) ;
    if(i<x->n && compare(*key , x->keys[i]) == 0){
        if(x->leaf){
            remove_key(x,i);
            x->n--;
            node_disk_write(myDB, x);
            return res;
        }
        else{ //if !leaf
            struct BTreeNode *a = node_disk_read(myDB,x->childs[i]);
            if (a->n >= myDB->t){//case 2,a
                struct DBT k,v;
                k.data = malloc(MAX_KEY_LENGTH);
                k.size = MAX_KEY_LENGTH; 
                v.data = malloc(MAX_VALUE_LENGTH);
                v.size = MAX_VALUE_LENGTH; 
              
                get_predecessor_key(myDB, a, key, &k, &v);
                
                memcpy(x->keys[i].data, k.data, MAX_KEY_LENGTH);
                x->keys[i].size = k.size;
                memcpy(x->values[i].data, v.data, MAX_VALUE_LENGTH);
                x->values[i].size = v.size;
                
                node_disk_write(myDB, x);
                res = delete_from_node(myDB, a, &k);
                
                free(k.data);
                free(v.data);
            }
            else {
                struct BTreeNode *b = node_disk_read(myDB,x->childs[i+1]);
                if (b->n >= myDB->t){//case 2,b
                    struct DBT k,v;
                    k.data = malloc(MAX_KEY_LENGTH);
                    k.size = MAX_KEY_LENGTH; 
                    v.data = malloc(MAX_VALUE_LENGTH);
                    v.size = MAX_VALUE_LENGTH; 
                    
                    get_successor_key(myDB, b, key, &k, &v);
                    
                    memcpy(x->keys[i].data, k.data, MAX_KEY_LENGTH);
                    x->keys[i].size = k.size;
                    memcpy(x->values[i].data, v.data, MAX_VALUE_LENGTH);
                    x->values[i].size = v.size;
                    
                    node_disk_write(myDB, x);
                    res = delete_from_node(myDB, b, &k);
                    
                    free(k.data);
                    free(v.data);
                }
                else if(a->n == myDB->t-1 && b->n == myDB->t-1){ // case 2,c
                        
                    merge_nodes(myDB, x, a , b , i);
                    
                    node_disk_write(myDB, a);
                    if(x==myDB->root && x->n==0){
                        node_free(myDB,b);
                        myDB->depth--;
                        myDB->root = a;
                        node_disk_delete(myDB,x->offset);
                        dbmetadata_disk_write(myDB,-1);//update root offset
                        node_free(myDB,x);
                        res = delete_from_node(myDB,a,key);
                        return res;
                    }
                      
                    node_disk_write(myDB, x);                    
                    res = delete_from_node(myDB,a,key);
                }
                else {
                    return 0;
                }
                node_free(myDB,b);
            }
            node_free(myDB,a);
            return res;
        }
    }
    else{
        if(x->leaf) {
            return 0;
        }
        struct BTreeNode *y = node_disk_read(myDB,x->childs[i]);
        if(y->n >= myDB->t){
            res = delete_from_node(myDB, y, key);
            node_free(myDB,y);
            return res;
        }
        else if(y->n == myDB->t-1){
            if(i>0){
                struct BTreeNode *a = node_disk_read(myDB,x->childs[i-1]);
                if(a->n >= myDB->t){
                    

                    for(j=y->n;j>0;j--){
                        copy_key(y,j, y,j-1);
                    }
                    for(j=y->n+1;j>0;j--){
                        y->childs[j] = y->childs[j-1];
                    }
                    copy_key(y,0, x,i-1);
                    
                    copy_key(x,i-1, a,a->n-1);
                    
                    y->childs[0] = a->childs[a->n];
                    a->n--;
                    y->n++;
                    
                    node_disk_write(myDB, x);
                    node_disk_write(myDB, a);
                    node_disk_write(myDB, y);
                    node_free(myDB,a);
                    
                    res = delete_from_node(myDB,y,key);
                    node_free(myDB,y);
                    return res;
                }
                else{
                    return delete_case3_helper(myDB,x,a,y,key,i);
                }
            }
            else{//i==0
                return delete_case3_helper(myDB,x,NULL,y,key,i);
            }
        }
        else {
            return 0;
        }
    }
}

int delete(struct DB *db, struct DBT * key){
    struct MyDB * myDB = (struct MyDB *) db;
    if(myDB->root->n == 0) fprintf(stderr,"DB is already empty\n");
    delete_from_node(myDB, myDB->root, key);
    return 0;
}
