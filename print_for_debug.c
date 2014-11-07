#include "print_for_debug.h"

#include "work_with_disk.h"
#include "node_alloc.h"

// --------------------- print for debug purposes ----------------
int get_current_n_r(const struct MyDB *myDB, const struct BTreeNode * x){
    int i,s = 0;
    if(x->leaf) return x->n;
    for(i=0;x->n>0 && i<x->n+1;i++){
        struct BTreeNode *c = node_disk_read(myDB,x->childs[i]);
        s +=  get_current_n_r(myDB, c);
        node_free(myDB,c);
    }
    return x->n + s;
}
int get_current_n(const struct DB *db){//how many keys in db?
    const struct MyDB *myDB = (const struct MyDB *) db;
    return get_current_n_r(myDB,myDB->root);
}
void print_DB_info(const struct DB *db){
    const struct MyDB *myDB = (const struct MyDB *) db; 
    printf("\nDATABASE INFO\n");
    printf("t -> %lu \n",myDB->t);
    printf("chunk_size -> %lu \n",myDB->chunk_size);
    printf("max_size -> %lu \n",myDB->max_size);
    printf("size -> %lu \n",myDB->size);
    printf("depth -> %d \n",myDB->depth);
    long i;
    char j;
    printf("-------EXISTS:\n");
    for(i=0;(double)i<myDB->size/8.0;i++){
        for(j=7;j>=0;j--){
            printf("%d",(myDB->exist[i] & (1 << j)) >> j);
        }
    }
    printf("\n||||||||--- ROOT NODE: ---|||||||||||\n");
    print_Node_info((const struct DB *)myDB, myDB->root);
}

void print_Node_info(const struct DB *db, struct BTreeNode *node){
    const struct MyDB *myDB = (const struct MyDB *) db;
    printf("NODE INFO\n");
    printf("offset -> %lu \n",node->offset);
    printf("n -> %lu \n",node->n);
    printf("isleaf -> %d \n",node->leaf);
    long i;
    for(i=0;i<node->n;i++){
        printf("%li key is \"%s\", ",i,(char *)(node->keys[i]).data);
        printf("%li value is \"%s\"\n",i,(char *)(node->values[i]).data);
    }
    if(!node->leaf && node->n>0) { 
        printf("\n-----------------MINI CHILDREN of %lu\n",node->offset);   
        for(i=0;i< node->n +1;i++){
            printf("child %li -> %lu\n",i,node->childs[i]);
        }
    }
    printf("\n-----------------FULL CHILDREN of %lu\n\n", node->offset);
    if(!node->leaf && node->n>0) { 
        for(i=0;i< node->n +1;i++){
            printf("\n|||||%li child is in %lu offset||||||\n",i,node->childs[i]);
            struct BTreeNode *c = node_disk_read(myDB, node->childs[i]);
            print_Node_info((const struct DB *)myDB, c);
            node_free(myDB,c);
        }
    }
    else printf("|||| no children :( ||||\n");
    printf("\n-----------------ENDOFCHILDREN of %lu\n\n",node->offset);
}

