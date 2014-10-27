#include "node_alloc.h"

#ifndef __MYDB__WORK_WITH_DISK__
    #define __MYDB__WORK_WITH_DISK__
    #include "work_with_disk.h"
#endif



struct BTreeNode *node_malloc(const struct MyDB *myDB){//Allocate without assignment offset in file
    struct BTreeNode *node = malloc(sizeof(struct BTreeNode));
    if(node == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC node\n");
        return node;
    }
    size_t t = myDB->t;
    node->keys = malloc((2*t-1)*sizeof(struct DBT));
    if(node->keys == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC node->keys\n");
        return node;
    }
    node->values = malloc((2*t-1)*sizeof(struct DBT)); 
    if(node->values == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC node->values\n");
        return node;
    }
    
    node->n = 0;
    node->leaf = true;
    node->offset = 0;
    long j;
    for(j=0;j<2*t-1;j++){
        node->keys[j].data =(char *) calloc(MAX_KEY_LENGTH,sizeof(char));
        if(node->keys[j].data == NULL){
            fprintf(stderr,"ERROR: CANNOT MALLOC node->keys[j].data\n");
            return node;
        }
        node->keys[j].size = 0;
        node->values[j].data =(char *) calloc(MAX_VALUE_LENGTH,sizeof(char));
        if(node->values[j].data == NULL){
            fprintf(stderr,"ERROR: CANNOT MALLOC node->values[j].data\n");
            return node;
        }
        node->values[j].size = 0;
    }
    node->childs = calloc(2*t,sizeof(size_t));
    if(node->childs == NULL){
            fprintf(stderr,"ERROR: CANNOT MALLOC node->childs\n");
            return node;
        }
    return node;
}

void node_free(const struct MyDB *myDB, struct BTreeNode *s){
    long i=0;
    for(i=0;i<2*myDB->t-1;i++){
        free(s->keys[i].data);
        free(s->values[i].data);
    }
    free(s->keys);
    free(s->values);
    free(s->childs);
    free(s);
}

int assign_BTreeNode(struct MyDB *myDB, struct BTreeNode *node){//assign existing (not in file) node's offset in file 
    long i;
    for(i=0; (i < myDB->size/8 + 1) && (i < myDB->max_size/8); i++)
        if(myDB->exist[i] !=  255 ) break;
    if(i>=myDB->max_size/8) return -1;
    char j = 7;
    while ( j>0 && ((myDB->exist[i] & (1 << j)) >> j)  == 1  ) j--;
    myDB->exist[i] = myDB->exist[i] | (1 << j);
    myDB->size ++;
    node->offset = i*8 + 7 - j;
    dbmetadata_disk_write(myDB, node->offset);
    return 0;
}

struct BTreeNode *create_BTreeNode(struct MyDB *myDB){//malloc and assign node's offset in file (in free position) 
    struct BTreeNode *node = node_malloc(myDB); 
    if (assign_BTreeNode(myDB,node)<0){
        fprintf(stderr,"ERROR: cannot assign node\n");
        node_free(myDB, node);
        return NULL;
    }
    return node;
}
