#include "db.h" 
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#define MAX_KEY_LENGTH  16
#define MAX_VALUE_LENGTH  32

//file = DB_METADATA + BTREE
// Covering BTREE
// BTREE = node + {node}
// chunk ::= node_metadata + data
// data ::= (key,value)
// ???array in memory = size_of_array + elements


int node_disk_write(struct MyDB *myDB, struct BTreeNode *node){
    int file_id = myDB->id_file;
    lseek(file_id, (node->offset + 1 + myDB->max_size/myDB->chunk_size ) * myDB->chunk_size, SEEK_SET); //+1 is because DB metadata in first chunk, next chunks for bitmask 
    size_t i=0;
    //NODE_METADATA
    memcpy((myDB->buffer+i),&(node->offset),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(node->leaf),sizeof(bool));
    i+=sizeof(bool);
    memcpy((myDB->buffer+i),&(node->n),sizeof(size_t));
    i+=sizeof(size_t);
    size_t j;
    
    if(node->n > 0)
        for(j=0; j < node->n + 1; j++){ //n+1 CHILDS
            memcpy((myDB->buffer+i+j*sizeof(size_t)),&(node->childs[j]),sizeof(size_t));
        }
    i+=2*(myDB->t)*sizeof(size_t);
    
    for(j=0; j< node->n; j++){//n    (klength,key)+(vlength,value) 
        memcpy((myDB->buffer+i),&(node->keys[j].size),sizeof(size_t));
        i+=sizeof(size_t);
        memcpy((myDB->buffer+i),&(node->keys[j].data),sizeof(MAX_KEY_LENGTH));
        i+=sizeof(MAX_KEY_LENGTH);
        memcpy((myDB->buffer+i),&(node->values[j].size),sizeof(size_t));
        i+=sizeof(size_t);
        memcpy((myDB->buffer+i),&(node->values[j].data),sizeof(MAX_VALUE_LENGTH));
        i+=sizeof(MAX_VALUE_LENGTH);
    }
    
    write(file_id, myDB->buffer , myDB->chunk_size*sizeof(char));
    return 0;
}

struct BTreeNode *node_disk_read(struct MyDB *myDB, size_t offset ){
    int file_id = myDB->id_file;
    struct BTreeNode *node = (struct BTreeNode *) malloc(sizeof(struct BTreeNode));//need to free somewhere
    
    lseek(file_id, (offset + 1 + myDB->max_size/myDB->chunk_size ) * myDB->chunk_size, SEEK_SET); //+1 is because DB metadata in first chunk, next chunks for bitmask 
    size_t i=0;
    read(file_id, myDB->buffer, myDB->chunk_size*sizeof(char));
    
    //NODE_METADATA
    memcpy(&(node->offset),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy(&(node->leaf),(myDB->buffer+i),sizeof(bool));
    i+=sizeof(bool);
    memcpy(&(node->n),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    size_t j;
    
    if(node->n > 0)
        for(j=0; j < node->n + 1; j++){ //n+1 CHILDS
            memcpy(&(node->childs[j]),(myDB->buffer+i+j*sizeof(size_t)),sizeof(size_t));
        }
    i+=2*(myDB->t)*sizeof(size_t);
    
    for(j=0; j< node->n; j++){//n    (klength,key)+(vlength,value) 
        memcpy(&(node->keys[j].size),(myDB->buffer+i),sizeof(size_t));
        i+=sizeof(size_t);
        memcpy(&(node->keys[j].data),(myDB->buffer+i),sizeof(MAX_KEY_LENGTH));
        i+=sizeof(MAX_KEY_LENGTH);
        memcpy(&(node->values[j].size),(myDB->buffer+i),sizeof(size_t));
        i+=sizeof(size_t);
        memcpy(&(node->values[j].data),(myDB->buffer+i),sizeof(MAX_VALUE_LENGTH));
        i+=sizeof(MAX_VALUE_LENGTH);
    }
    
    return node;
}

struct BTreeNode *create_BTreeNode(struct MyDB *myDB,bool is_leaf){
    struct BTreeNode *node = (struct BTreeNode *) malloc(sizeof(struct BTreeNode)); //need to free somewhere
    size_t t = myDB->t;
    node->leaf = is_leaf;
    node->n = 0;
    node->keys = (struct DBT *) calloc(2*t-1,sizeof(struct DBT));
    node->values = (struct DBT *) calloc(2*t-1,sizeof(struct DBT)); 
    size_t j;
    for(j=0;j<2*t-1;j++){
        node->keys[j].data = calloc(MAX_KEY_LENGTH,sizeof(char));
        node->keys[j].size = 0;
        node->values[j].data = calloc(MAX_VALUE_LENGTH,sizeof(char));
        node->values[j].size = 0;
    }
    node->childs = (size_t *) calloc(2*t,sizeof(size_t));
    
    size_t i;
    size_t size = myDB->size;
    for(i=0; (i < size+1) && (i < myDB->max_size); i++)
        if(myDB->exist[i]==false) break;
    if(i==myDB->max_size) return NULL;   
    myDB->size = size+1;
    myDB->exist[i] = true;
    node->offset = i;
    return node;
}

struct DB *dbcreate(const char *file, const struct DBC *conf){
    int file_id = creat(file,0);
    struct MyDB *myDB = (struct MyDB *) malloc(sizeof(struct MyDB)); //need to free somewhere
    myDB->id_file = file_id;
    
    //node metadata = sizeof(offset+leaf+n)+ (n+1)*sizeof(child)+ n*sizeof(k+v)
    myDB->chunk_size = conf->chunk_size;
    
    int max_node_keys = (myDB->chunk_size - 3*sizeof(size_t)-sizeof(bool))/(sizeof(size_t) + MAX_KEY_LENGTH + MAX_VALUE_LENGTH); // max_node_keys == 2t-1
    if(max_node_keys % 2 == 0) max_node_keys -= 1;
    myDB->t = (max_node_keys+1)/2;
    
    myDB->size = 0;
    myDB->max_size = ( (conf->db_size/myDB->chunk_size-1)/(myDB->chunk_size+1) ) * myDB->chunk_size;//first chunk - DB metadata
    myDB->exist =(bool *) calloc(myDB->max_size, sizeof(bool));
    
    size_t i;
    for(i=0; i<myDB->max_size; i++)
        myDB->exist[i] = false;
    
    //DB_METADATA = 1 chunk () + m chunk (exist[])
    myDB->buffer = calloc(myDB->chunk_size, sizeof(char));
    i=0;
    memcpy((myDB->buffer+i),&(myDB->chunk_size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->t),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->max_size),sizeof(size_t));
    i+=sizeof(size_t);
    
    write(file_id, myDB->buffer , myDB->chunk_size*sizeof(char)); //first main chunk with metadata in file
    
    //next in file (max_size/chunk_size) pages of bitmask myDB->exist
    memset(myDB->buffer,0,myDB->chunk_size*sizeof(char));
    size_t j=0;
    for(j=0; j < myDB->max_size / myDB->chunk_size; j++)
        write(file_id, myDB->buffer,myDB->chunk_size*sizeof(char));
    
    myDB->root = create_BTreeNode(myDB,true);//creation of root
    node_disk_write(myDB,myDB->root);
    return (struct DB *)myDB;
}

struct DB *dbopen  (const char *file){
    int file_id = open(file, O_RDWR | O_APPEND, 0);
    struct MyDB *myDB = (struct MyDB *) malloc(sizeof(struct MyDB));//need to free somewhere
    myDB->id_file = file_id;
    
    read(file_id,&(myDB->chunk_size) ,sizeof(size_t));
    myDB->buffer = (char *) calloc(myDB->chunk_size,sizeof(char)); //need to free somewhere
    
    size_t i = 0;
    //chunk_size already in myDB
    i+=sizeof(size_t);
    memcpy(&(myDB->t),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy(&(myDB->size),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy(&(myDB->max_size),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    
    size_t j;
    //next in file (max_size/chunk_size) pages of bitmask myDB->exist
    myDB->exist = calloc(myDB->max_size,sizeof(char));
    i=0;
    for(j=0;j < myDB->max_size / myDB->chunk_size; j++){
        read(file_id, myDB->buffer,myDB->chunk_size*sizeof(char));
        memcpy(((myDB->exist)+i),myDB->buffer,sizeof(char));
        i+=myDB->chunk_size*sizeof(char);
    }
    
    myDB->root = (struct BTreeNode *)malloc(sizeof(struct BTreeNode)); //need to free somewhere 
    read(file_id, myDB->buffer,myDB->chunk_size*sizeof(char));
    memcpy(myDB->root,myDB->buffer,myDB->chunk_size*sizeof(char));
    
    return (struct DB *)myDB;
}

int split_child(struct MyDB *myDB, struct BTreeNode *x,size_t i){
    //if(myDB->size <= ???) return 0; 
    struct BTreeNode *y = node_disk_read(myDB, x->childs[i]);
    struct BTreeNode *z = create_BTreeNode(myDB,y->leaf);
    z->n = myDB->t - 1;
    
    size_t j;
    for(j=0; j < myDB->t - 1; j++){
        memcpy(z->keys[j].data,y->keys[j + myDB->t].data, MAX_KEY_LENGTH);
        z->keys[j].size = y->keys[j + myDB->t].size;
        memcpy(z->values[j].data,y->values[j + myDB->t].data, MAX_VALUE_LENGTH);
        z->values[j].size = y->values[j + myDB->t].size;
    }
    if(!y->leaf){
        for(j=0; j < myDB->t; j++ )
            z->childs[j] = y->childs[j + myDB->t];
    }
    y->n = myDB->t-1;
    for(j=x->n ; j>=i; j--)
        x->childs[j+1] = x->childs[j];
    x->childs[i+1] = z->offset;
    for(j=x->n-1; j>=i-1;j--){
        memcpy(x->keys[j+1].data,x->keys[j].data, MAX_KEY_LENGTH);
        x->keys[j+1].size = x->keys[j].size;
        memcpy(x->values[j+1].data,x->values[j].data, MAX_VALUE_LENGTH);
        x->values[j+1].size = x->values[j].size;
    }
    memcpy(x->keys[i].data,y->keys[myDB->t].data, MAX_KEY_LENGTH);
    x->keys[i].size = y->keys[myDB->t].size;
    memcpy(x->values[i].data,y->values[myDB->t].data, MAX_VALUE_LENGTH);
    x->values[i].size = y->values[myDB->t].size;
    
    x->n +=1;
    node_disk_write(myDB,y);
    node_disk_write(myDB,z);
    node_disk_write(myDB,x);
    return 0;
}

int compare(const struct DBT k1, const struct DBT k2){// !!!!!!!!!!!!!!!!!!!!
    if(k1.size == k2.size){
        return strncmp(k1.data,k2.data, k1.size);//or strncmp
    }
    else if(k1.size > k2.size){
        size_t diff = k1.size-k2.size;
        char *r2 = malloc(k1.size * sizeof(char));
        size_t i;
        for(i=0;i<diff;i++)
            strncpy((r2+i),"0",1);
        r2[i]='\0';    
        strncat(r2,k2.data,k2.size);//or strncat
        int result = strncmp(k1.data, r2, k1.size);
        free(r2);
        return result;
    }
    else {
        size_t diff = k2.size-k1.size;
        char *r1 = malloc(k2.size * sizeof(char));
        size_t i;
        for(i=0;i<diff;i++)
            strncpy((r1+i),"0",1);
        r1[i]='\0';    
        strncat(r1,k1.data,k1.size);//or strncat
        int result = strncmp(r1,k2.data, k2.size);
        free(r1);
        return result;
    }
}

int insert_nonfull(struct MyDB *myDB, struct BTreeNode *x, struct DBT *key, struct DBT *data){
    size_t i = x->n;
    if(x->leaf){
        while(i>=0 && compare(*key,x->keys[i])<0 ){
            memcpy(x->keys[i+1].data,x->keys[i].data, MAX_KEY_LENGTH);
            x->keys[i+1].size = x->keys[i].size;
            i-=1;
        }
        memcpy(x->keys[i+1].data, key->data, (key->size < MAX_KEY_LENGTH) ? key->size : MAX_KEY_LENGTH);
        x->keys[i+1].size = key->size;
        memcpy(x->values[i+1].data,data->data, (data->size < MAX_VALUE_LENGTH) ? data->size : MAX_VALUE_LENGTH);
        x->values[i+1].size = data->size;
        x->n += 1;
        node_disk_write(myDB,x);
    }
    else{
        while(i>=0 && compare(*key,x->keys[i])<0)
            i-=1;
        i=+1;
        struct BTreeNode *c = node_disk_read(myDB, x->childs[i]); // !!!!!!!!!!!!!!
        if(c->n == 2*(myDB->t)-1){
            split_child(myDB,x,i);
            if(compare(*key, x->keys[i])>0) i+=1;
        }
        insert_nonfull(myDB, c, key, data);
        free(c);
    }
}

int insert(struct DB *db, struct DBT *key, struct DBT *data){
    struct MyDB *myDB = (struct MyDB *)db;
    struct BTreeNode *r = myDB->root;
    if(myDB->root->n == 2*(myDB->t)-1){
        struct BTreeNode *s = create_BTreeNode(myDB,false);
        struct BTreeNode *temp = create_BTreeNode(myDB,r->leaf);
        r->offset = temp->offset;
        node_disk_write(myDB,r);
        s->n = 0;
        s->childs[0] = r->offset;
        myDB->root = s;
        split_child(myDB,s,0);
        insert_nonfull(myDB, s, key, data);
    }
    else insert_nonfull(myDB, r, key ,data);
    return 0;
}


void print_DB_info(struct MyDB *myDB){
    printf("\nDATABASE INFO\n");
    printf("t -> %lu \n",myDB->t);
    printf("chunk_size -> %lu \n",myDB->chunk_size);
    printf("max_size -> %lu \n",myDB->max_size);
    printf("size -> %lu \n",myDB->size);
}

void print_Node_info(struct BTreeNode *node){
    printf("\nNODE INFO\n");
    printf("offset -> %lu \n",node->offset);
    printf("n -> %lu \n",node->n);
    printf("isleaf -> %d \n",node->leaf);
    int i;
    for(i=0;i<node->n;i++){
        printf("%d key is \"%s\"\n",i,(char *)(node->keys[i]).data);
        printf("%d value is \"%s\"\n",i,(char *)(node->values[i]).data);
    }
    for(i=0;i< node->n +1;i++)
        printf("%d child is in %lu offset\n",i,node->childs[i]);
}

int main(){
    struct DBC dbc;
    dbc.db_size = 512 * 1024 * 1024;
    dbc.chunk_size = 4096;
    dbc.mem_size = 16 * 1024 * 1024;
    struct MyDB *db = (struct MyDB *) dbcreate("temp.db",&dbc);
    struct DBT key,value;
    
    key.data = "15";
    key.size = sizeof("15");
    value.data = "fuck_it";
    value.size = sizeof("fuck_it");
    insert((struct DB *)db,&key,&value);
    
    key.data = "9";
    key.size = sizeof("9");
    value.data = "myGod";
    value.size = sizeof("myGod");
    insert((struct DB *)db,&key,&value);
    
    key.data = "25";
    key.size = sizeof("25");
    value.data = "twenty five";
    value.size = sizeof("twenty five");
    insert((struct DB *)db,&key,&value);
    
    
    print_DB_info(db);
    print_Node_info(db->root);
    
    return 0;
}
