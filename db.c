#include "db.h" 
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#define MAX_KEY_LENGTH  64
#define MAX_VALUE_LENGTH  64

//file = DB_METADATA + BTREE
// Covering BTREE
// BTREE = node + {node}
// chunk ::= node_metadata + data
// data ::= n*(sizeofkey+key,sizeofvalue+value)


void print_Node_info(struct MyDB *myDB,struct BTreeNode *node);
void print_DB_info(struct MyDB *myDB);

struct BTreeNode *node_malloc(struct MyDB *myDB);
int node_free(struct MyDB *myDB, struct BTreeNode *s);
int assign_BTreeNode(struct MyDB *myDB, struct BTreeNode *node);
struct BTreeNode *create_BTreeNode(struct MyDB *myDB);
int node_disk_write(struct MyDB *myDB, struct BTreeNode *node);
struct BTreeNode *node_disk_read(struct MyDB *myDB, size_t offset );
int dbmetadata_disk_write(struct MyDB *myDB, long index );


// offset(number of node) to real offset in file
size_t convert_offset(struct MyDB *myDB, size_t offset){
    return (offset + 1 + myDB->max_size/myDB->chunk_size ) * myDB->chunk_size; //+1 is because DB metadata in first chunk, next chunks for bitmask
}

// ------------- CREATION OF NODE --------------

struct BTreeNode *node_malloc(struct MyDB *myDB){//Allocate without assignment offset in file
    struct BTreeNode *node = malloc(sizeof(struct BTreeNode));
    size_t t = myDB->t;
    node->keys = malloc((2*t-1)*sizeof(struct DBT));
    node->values = malloc((2*t-1)*sizeof(struct DBT)); 
    node->n = 0;
    node->leaf = true;
    node->offset = 0;
    long j;
    for(j=0;j<2*t-1;j++){
        node->keys[j].data =(char *) calloc(MAX_KEY_LENGTH,sizeof(char));
        node->keys[j].size = 0;
        node->values[j].data =(char *) calloc(MAX_VALUE_LENGTH,sizeof(char));
        node->values[j].size = 0;
    }
    node->childs = calloc(2*t,sizeof(size_t));
    return node;
}

int node_free(struct MyDB *myDB, struct BTreeNode *s){
    long i=0;
    for(i=0;i<2*myDB->t-1;i++){
        free(s->keys[i].data);
        free(s->values[i].data);
    }
    free(s->keys);
    free(s->values);
    free(s->childs);
    free(s);
    return 0;
}

int assign_BTreeNode(struct MyDB *myDB, struct BTreeNode *node){//assign existing (not in file) node's offset in file 
    long i;
    for(i=0; (i < myDB->size+1) && (i < myDB->max_size); i++)
        if(myDB->exist[i]==false) break;
    if(i>=myDB->max_size) return -1;   
    myDB->size ++;
    myDB->exist[i] = true;
    
    dbmetadata_disk_write(myDB,i);
    node->offset = i;
    return 0;
}

struct BTreeNode *create_BTreeNode(struct MyDB *myDB){//malloc and assign node's offset in file (in free position) 
    struct BTreeNode *node = node_malloc(myDB); 
    assign_BTreeNode(myDB,node);
    return node;
}


// ------------------ DISK OPERATIONS ----------------- 
int node_disk_write(struct MyDB *myDB, struct BTreeNode *node){
    int file_id = myDB->id_file;
    lseek(file_id, convert_offset(myDB, node->offset), SEEK_SET);  
    long i=0;
    //NODE_METADATA
    memcpy((myDB->buffer+i),&(node->offset),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(node->leaf),sizeof(bool));
    i+=sizeof(bool);
    memcpy((myDB->buffer+i),&(node->n),sizeof(size_t));
    i+=sizeof(size_t);
    long j;
    
    if(node->n > 0){ 
        for(j=0; j < node->n + 1; j++){ //n+1 CHILDS
            memcpy((myDB->buffer+i), &(node->childs[j]), sizeof(size_t));
            i+=sizeof(size_t);
        }
        
        for(j=0; j< node->n; j++){//n    (klength,key)+(vlength,value) 
            memcpy((myDB->buffer+i), &(node->keys[j].size), sizeof(size_t));
            i+=sizeof(size_t);
        }
        
        for(j=0; j< node->n; j++){
            memcpy((myDB->buffer+i), node->keys[j].data, MAX_KEY_LENGTH);
            i+=MAX_KEY_LENGTH;
        }
        
        for(j=0; j< node->n; j++){
            memcpy((myDB->buffer+i), &(node->values[j].size), sizeof(size_t));
            i+=sizeof(size_t);
        }
        
        for(j=0; j< node->n; j++){
            memcpy((myDB->buffer+i), node->values[j].data, MAX_VALUE_LENGTH);
            i+=MAX_VALUE_LENGTH;
        }
    }
    
    write(file_id, myDB->buffer , myDB->chunk_size*sizeof(char));
    return 0;
}


struct BTreeNode *node_disk_read(struct MyDB *myDB, size_t offset ){
    int file_id = myDB->id_file;
    if(file_id<0) fprintf(stderr,"NOT VALID FILE DESCRIPTOR\n");
    struct BTreeNode *node = node_malloc(myDB);
    
    lseek(file_id, convert_offset(myDB,offset), SEEK_SET);  
    long i=0;
    read(file_id, myDB->buffer, myDB->chunk_size*sizeof(char));

    //NODE_METADATA
    memcpy(&(node->offset),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy(&(node->leaf),(myDB->buffer+i),sizeof(bool));
    i+=sizeof(bool);
    memcpy(&(node->n),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    long j;
    
    if(node->n > 0){ 
        for(j=0; j < node->n + 1; j++){ //n+1 CHILDS
            memcpy(&(node->childs[j]),(myDB->buffer+i), sizeof(size_t));
            i+=sizeof(size_t);
        }

        for(j=0; j< node->n; j++){//n    (klength,key)+(vlength,value) 
            memcpy(&(node->keys[j].size), (myDB->buffer+i), sizeof(size_t));
            i+=sizeof(size_t);
        }
        
        for(j=0; j< node->n; j++){
            memcpy(node->keys[j].data, (myDB->buffer+i), MAX_KEY_LENGTH);
            i+=MAX_KEY_LENGTH;
        }
        
        for(j=0; j< node->n; j++){
            memcpy(&(node->values[j].size), (myDB->buffer+i), sizeof(size_t));
            i+=sizeof(size_t);
        }
        
        for(j=0; j< node->n; j++){
            memcpy(node->values[j].data, (myDB->buffer+i), MAX_VALUE_LENGTH);
            i+=MAX_VALUE_LENGTH;
        }
    }
    return node;
}

int dbmetadata_disk_write(struct MyDB *myDB, long index ){//update DB metadata
    long i=0;
    memcpy((myDB->buffer+i),&(myDB->chunk_size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->t),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->max_size),sizeof(size_t));
    i+=sizeof(size_t);
    if(myDB->root != NULL )
        memcpy((myDB->buffer+i),&(myDB->root->offset),sizeof(size_t));
    else memset((myDB->buffer+i),0,sizeof(size_t));
    
    lseek(myDB->id_file, 0, SEEK_SET);
    write(myDB->id_file, myDB->buffer , myDB->chunk_size*sizeof(char)); //first main chunk with metadata in file
    if(index>=0 && index<myDB->max_size){
        //next in file (max_size/chunk_size) pages of bitmask myDB->exist
        long j = index / myDB->chunk_size;
        for(i=0; i < myDB->chunk_size; i++)
            myDB->buffer[i] = myDB->exist[i+j*myDB->chunk_size];
        lseek(myDB->id_file, j*myDB->chunk_size, SEEK_CUR);
        write(myDB->id_file, myDB->buffer,myDB->chunk_size*sizeof(char));
    }
    return 0;
}


// ----------------WORK WITH DB ---------------------

struct DB *dbcreate(const char *file, const struct DBC *conf){
    unlink(file);
    int file_id = open(file,  O_CREAT |O_RDWR |O_TRUNC ,0);
    if(file_id<0) fprintf(stderr,"NOT VALID FILE DESCRIPTOR\n");
    struct MyDB *myDB = (struct MyDB *) malloc(sizeof(struct MyDB)); 
    myDB->id_file = file_id;
    
    myDB->chunk_size = conf->chunk_size;
    int max_node_keys = (myDB->chunk_size - 3*sizeof(size_t)-sizeof(bool))/(3*sizeof(size_t) + MAX_KEY_LENGTH + MAX_VALUE_LENGTH); // max_node_keys == 2t-1
    if(max_node_keys % 2 == 0) max_node_keys -= 1;
    myDB->t = (max_node_keys+1)/2;
    myDB->size = 0;
    myDB->max_size = ( (conf->db_size/myDB->chunk_size-1)/(myDB->chunk_size+1) ) * myDB->chunk_size;//first chunk - DB metadata
    myDB->buffer = calloc(myDB->chunk_size, sizeof(char));
    myDB->exist =(bool *) calloc(myDB->max_size, sizeof(bool));
    long i;
    for(i=0; i<myDB->max_size; i++)
        myDB->exist[i] = false;
    myDB->root = create_BTreeNode(myDB);//creation of root //myDB->root->leaf = true  and  myDB->offset = 0 by default in node_malloc() 
    
    //node metadata = sizeof(offset+leaf+n) + (n+1)*sizeof(child) + n*sizeof(k+v+!!!!!!!!!16!!!!!!!!!!)
    
    //DB_METADATA = 1 chunk () + m chunk (exist[])
    i=0;
    memcpy((myDB->buffer+i),&(myDB->chunk_size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->t),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->max_size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->root->offset),sizeof(size_t));//myDB->root->offset 
    i+=sizeof(size_t);
    
    write(file_id, myDB->buffer , myDB->chunk_size*sizeof(char)); //first main chunk with metadata in file
    
    //next in file (max_size/chunk_size) pages of bitmask myDB->exist
    memset(myDB->buffer,0,myDB->chunk_size*sizeof(char));
    long j=0;
    for(j=0; j < myDB->max_size / myDB->chunk_size; j++)
        write(file_id, myDB->buffer,myDB->chunk_size*sizeof(char));
    
    node_disk_write(myDB,myDB->root);
    return (struct DB *)myDB;
}

struct DB *dbopen  (const char *file){
    int file_id = open(file, O_RDWR | O_APPEND, 0);
    if(file_id<0) fprintf(stderr,"NOT VALID FILE DESCRIPTOR\n");
    struct MyDB *myDB = (struct MyDB *) malloc(sizeof(struct MyDB));
    myDB->id_file = file_id;
    
    read(file_id,&(myDB->chunk_size) ,sizeof(size_t));
    
    myDB->buffer = (char *) calloc(myDB->chunk_size,sizeof(char)); 
    myDB->exist = calloc(myDB->max_size,sizeof(bool));
    
    long i = 0;
    //chunk_size already in myDB
    i+=sizeof(size_t);
    memcpy(&(myDB->t),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy(&(myDB->size),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy(&(myDB->max_size),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    size_t root_offset;
    memcpy(&(root_offset),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    myDB->root = node_disk_read(myDB,root_offset);
    
    long j;
    //next in file (max_size/chunk_size) pages of bitmask myDB->exist
    for(j=0;j < myDB->max_size / myDB->chunk_size; j++){
        read(file_id, myDB->buffer,myDB->chunk_size*sizeof(char));
        for(i=0;i<myDB->chunk_size;i++)
            myDB->exist[i+j*myDB->chunk_size] = myDB->buffer[i];
    }
    
    return (struct DB *)myDB;
}

int dbclose(struct DB *db){
    struct MyDB *myDB = (struct MyDB *) db;
    close(myDB->id_file);
    node_free(myDB,myDB->root);
    free(myDB->buffer);
    free(myDB->exist);
    return 0;  
}


// -------------------- BTREE ALGORITHMS --------------

int split_child(struct MyDB *myDB, struct BTreeNode *x,long i){
    struct BTreeNode *y = node_disk_read(myDB, x->childs[i]);
    struct BTreeNode *z = create_BTreeNode(myDB);
    z->leaf = y->leaf;
    z->n = myDB->t - 1;
    long j;
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
    if(x->n!=0){
        for(j=x->n-1; j>=i-1; j--){
            memcpy(x->keys[j+1].data,x->keys[j].data, MAX_KEY_LENGTH);
            x->keys[j+1].size = x->keys[j].size;
            memcpy(x->values[j+1].data,x->values[j].data, MAX_VALUE_LENGTH);
            x->values[j+1].size = x->values[j].size;
        }
    }
    memcpy(x->keys[i].data,y->keys[myDB->t - 1].data, MAX_KEY_LENGTH);
    x->keys[i].size = y->keys[myDB->t -1 ].size;
    memcpy(x->values[i].data,y->values[myDB->t - 1].data, MAX_VALUE_LENGTH);
    x->values[i].size = y->values[myDB->t - 1].size;
    
    x->n ++;
        
    node_disk_write(myDB,y);
    node_disk_write(myDB,z);
    node_disk_write(myDB,x);
    
    node_free(myDB, y);
    node_free(myDB, z);
    return 0;
}

int compare(const struct DBT k1, const struct DBT k2){// key = string, representing number. example: "12", "145". P.S."00002323" == "2323"
    if(k1.size == k2.size){
        return strncmp(k1.data,k2.data, k1.size);
    }
    else if(k1.size > k2.size){
        size_t diff = k1.size-k2.size;
        char *r2 = malloc(k1.size * sizeof(char));
        long i;
        for(i=0;i<diff;i++)
            strncpy((r2+i),"0",1);
        r2[i]='\0';    
        strncat(r2,k2.data,k2.size);
        int result = strncmp(k1.data, r2, k1.size);
        free(r2);
        return result;
    }
    else {
        size_t diff = k2.size-k1.size;
        char *r1 = malloc(k2.size * sizeof(char));
        long i;
        for(i=0;i<diff;i++)
            strncpy((r1+i),"0",1);
        r1[i]='\0';    
        strncat(r1,k1.data,k1.size);
        int result = strncmp(r1,k2.data, k2.size);
        free(r1);
        return result;
    }
}

int insert_nonfull(struct MyDB *myDB, struct BTreeNode *x, struct DBT *key, struct DBT *data){
    long i = x->n;
    if(x->leaf){
       while(i>=1 && compare(*key , x->keys[i-1])<0 ){
            memcpy(x->keys[i].data,x->keys[i-1].data, MAX_KEY_LENGTH);
            memcpy(&(x->keys[i].size), &(x->keys[i-1].size),sizeof(size_t));
            memcpy(x->values[i].data,x->values[i-1].data, MAX_VALUE_LENGTH);
            memcpy(&(x->values[i].size), &(x->values[i-1].size),sizeof(size_t));
            i--;
        }
        memcpy(x->keys[i].data, key->data, (key->size < MAX_KEY_LENGTH) ? key->size : MAX_KEY_LENGTH);
        memcpy(&(x->keys[i].size), &(key->size),sizeof(size_t));
        memcpy(x->values[i].data,data->data, (data->size < MAX_VALUE_LENGTH) ? data->size : MAX_VALUE_LENGTH);
        memcpy(&(x->values[i].size), &(data->size),sizeof(size_t));
        x->n ++;
        node_disk_write(myDB,x);
    }
    else{
        while(i>=1 && compare(*key , x->keys[i-1])<0)
            i--;
        i++;
                
        struct BTreeNode *c = node_disk_read(myDB, x->childs[i-1]); 

        if(c->n == 2*(myDB->t)-1){
            split_child(myDB,x,i-1);
            if(compare(*key, x->keys[i-1])>0) i++;
        }
        insert_nonfull(myDB, c, key, data);
        node_free(myDB,c);
    }
    return 0;
}

int insert(struct DB *db, struct DBT *key, struct DBT *data){
    struct MyDB *myDB = (struct MyDB *)db;
    struct BTreeNode *r = myDB->root;
    if(myDB->root->n == 2*(myDB->t)-1){
        struct BTreeNode *s = create_BTreeNode(myDB);
        s->leaf = false;
        myDB->root = s;
        dbmetadata_disk_write(myDB,-1);
        //s->n = 0; by default in node_malloc
        s->childs[0] = r->offset;
        node_disk_write(myDB,r);
        node_disk_write(myDB,s);
        
        node_free(myDB,r);
        
        split_child(myDB,s,0);
        
        insert_nonfull(myDB, s, key, data);
    }
    else {
        insert_nonfull(myDB, r, key ,data);
    }
    return 0;
}


// --------------------- print for debug purposes ----------------

void print_DB_info(struct MyDB *myDB){
    printf("\nDATABASE INFO\n");
    printf("t -> %lu \n",myDB->t);
    printf("chunk_size -> %lu \n",myDB->chunk_size);
    printf("max_size -> %lu \n",myDB->max_size);
    printf("size -> %lu \n",myDB->size);
    printf("--- ROOT NODE: ---");
    print_Node_info(myDB, myDB->root);
}

void print_Node_info(struct MyDB *myDB, struct BTreeNode *node){
    printf("NODE INFO\n");
    printf("offset -> %lu \n",node->offset);
    printf("n -> %lu \n",node->n);
    printf("isleaf -> %d \n",node->leaf);
    int i;
    for(i=0;i<node->n;i++){
        printf("%d key is \"%s\"\n",i,(char *)(node->keys[i]).data);
        printf("%d value is \"%s\"\n",i,(char *)(node->values[i]).data);
    }
    if(!node->leaf && node->n>0) {    
        for(i=0;i< node->n +1;i++){
            printf("\n|||||%d child is in %lu offset||||||\n",i,node->childs[i]);
            struct BTreeNode *c = node_disk_read(myDB, node->childs[i]);
            print_Node_info(myDB, c);
            node_free(myDB,c);
        }
    }
    else printf("|||| no children :( ||||\n");
}


// --------------------- Main for debug purposes ---------------
int main(){
    struct DBC dbc;
    dbc.db_size = 512 * 1024 * 1024;
    dbc.chunk_size = 4096;
    dbc.mem_size = 16 * 1024 * 1024;
    struct MyDB *db = (struct MyDB *) dbcreate("temp.db",&dbc);
    struct DBT key,value;
    
    print_DB_info(db);
    int j=0;
    char buf[3];
    buf[2] = '\0';
    printf("\nAFTER INSERT:\n");
    for(j=0;j<30;j++) {
        key.data = buf;
        *((char *)key.data + 1) = 48 + j % 10;
        *((char *)key.data) = 48 + ((j<10) ? 1 : ((j<20) ? 2 : ((j<30) ? 3 : 4)));
        key.size = strlen(key.data)+1;
        value.data = "blablablah";
        value.size = strlen(value.data)+1;
        insert((struct DB *)db,&key,&value);
        
    }
    
    print_DB_info(db);
    return 0;
}
