#include "db.h" 
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include <time.h>

#define MAX_KEY_LENGTH  22
#define MAX_VALUE_LENGTH  22

//file = DB_METADATA + BTREE
// Covering BTREE
// BTREE = node + {node}
// chunk ::= node_metadata + data
// data ::= n*(sizeofkey+key,sizeofvalue+value)


void print_Node_info(const struct DB *db,struct BTreeNode *node);
void print_DB_info(const struct DB *db);

struct BTreeNode *node_malloc(const struct MyDB *myDB);
void node_free(const struct MyDB *myDB, struct BTreeNode *s);
int assign_BTreeNode(struct MyDB *myDB, struct BTreeNode *node);
struct BTreeNode *create_BTreeNode(struct MyDB *myDB);
int node_disk_write(struct MyDB *myDB, struct BTreeNode *node);
struct BTreeNode *node_disk_read(const struct MyDB *myDB, size_t offset );
int dbmetadata_disk_write(struct MyDB *myDB, long index );

int insert(struct DB *db, struct DBT *key, struct DBT *data);
int search(const struct DB *db, struct DBT *key, struct DBT *data);
int delete(struct DB *db, struct DBT * key);

int delete_from_node(struct MyDB *myDB, struct BTreeNode *x, struct DBT * key);



// offset(number of node) to real offset in file
size_t convert_offset(const struct MyDB *myDB, size_t offset){
    size_t temp = ((myDB->max_size/8) < myDB->chunk_size) ? 1 : (myDB->max_size/8)/myDB->chunk_size;
    return (offset + 1 + temp) * myDB->chunk_size; //+1 is because DB metadata in first chunk, next chunks for bitset
}

void copy_key(struct BTreeNode *x, long i, struct BTreeNode *y, long j ){
    memcpy(x->keys[i].data, y->keys[j].data, MAX_KEY_LENGTH);
    x->keys[i].size = y->keys[j].size;
    memcpy(x->values[i].data, y->values[j].data, MAX_VALUE_LENGTH);
    x->values[i].size = y->values[j].size;
}

// ------------- CREATION OF NODE --------------

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


// ------------------ DISK OPERATIONS ----------------- 
int node_disk_write(struct MyDB *myDB, struct BTreeNode *node){
    int id_file = myDB->id_file;
    if(id_file<0){
        fprintf(stderr,"ERROR: NOT VALID FILE DESCRIPTOR\n");
        return -1;
    }
    if(lseek(id_file, convert_offset(myDB, node->offset), SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return -1;
    }  
    long i=0;
    //NODE_METADATA
    memcpy((myDB->buffer+i),&(node->offset),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(node->leaf),sizeof(bool));
    i+=sizeof(bool);
    memcpy((myDB->buffer+i),&(node->n),sizeof(size_t));
    i+=sizeof(size_t);
    long j;
    
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
    
    if(write(id_file, myDB->buffer , myDB->chunk_size) != myDB->chunk_size){
        fprintf(stderr,"ERROR: write()\n");
        return -1;
    }
    return 0;
}


struct BTreeNode *node_disk_read(const struct MyDB *myDB, size_t offset ){
    int id_file = myDB->id_file;
    if(id_file<0){
        fprintf(stderr,"NOT VALID FILE DESCRIPTOR\n");
        return NULL;
    }
    struct BTreeNode *node = node_malloc(myDB);
    
    if(lseek(id_file, convert_offset(myDB, offset), SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return NULL;
    }
    long i=0;
    if(read(id_file, myDB->buffer, myDB->chunk_size) != myDB->chunk_size){
        fprintf(stderr,"ERROR: reed()\n");
        return NULL;
    }

    //NODE_METADATA
    memcpy(&(node->offset),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy(&(node->leaf),(myDB->buffer+i),sizeof(bool));
    i+=sizeof(bool);
    memcpy(&(node->n),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    long j;
    
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
    if(myDB->root != NULL ) memcpy((myDB->buffer+i),&(myDB->root->offset),sizeof(size_t));
    else memset((myDB->buffer+i),0,sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->depth),sizeof(int));
    i+=sizeof(int);
    
    
    if(lseek(myDB->id_file, 0, SEEK_SET) < 0){
        fprintf(stderr,"ERROR: lseek()\n");
        return -1;
    }
    if(write(myDB->id_file, myDB->buffer , myDB->chunk_size) != myDB->chunk_size){ //first main chunk with metadata in file
        fprintf(stderr,"ERROR: write()\n");
        return -1;
    }
    if(index>=0 && index<myDB->max_size){
        //next in file (max_size/8)/chunk_size pages of bitset myDB->exist
        long j = (index / 8) / myDB->chunk_size;
        for(i=0; i < myDB->chunk_size; i++)
            myDB->buffer[i] = myDB->exist[i+j*myDB->chunk_size];
        if(lseek(myDB->id_file, j*myDB->chunk_size, SEEK_CUR) < 0){
            fprintf(stderr,"ERROR: lseek()\n");
            return -1;
        }
        if(write(myDB->id_file, myDB->buffer, myDB->chunk_size) != myDB->chunk_size ){
            fprintf(stderr,"ERROR: write()\n");
            return -1;
        }
    }
    return 0;
}

int node_disk_delete(struct MyDB *myDB, size_t offset){
    int id_file = myDB->id_file;
    if(id_file<0){
        fprintf(stderr,"NOT VALID FILE DESCRIPTOR\n");
        return -1;
    }
    if(lseek(id_file, myDB->chunk_size, SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return -1;
    }
    //next in file (max_size/8)/chunk_size pages of bitset myDB->exist
    long j = (offset / 8)/myDB->chunk_size;
    if(lseek(id_file, myDB->chunk_size*j, SEEK_CUR)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return -1;
    }
    if(read(id_file, myDB->buffer, myDB->chunk_size) != myDB->chunk_size){
        fprintf(stderr,"ERROR: reed()\n");
        return -1;
    }
    int n = (offset/8) % myDB->chunk_size; //No of byte
    byte b = myDB->buffer[n];
    int i = offset % 8;
    b = ~(1 << (7-i)) & b;
    myDB->buffer[n] = b;
    if(lseek(id_file, myDB->chunk_size*(j+1), SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return -1;
    }
    if(write(myDB->id_file, myDB->buffer,myDB->chunk_size) != myDB->chunk_size ){
        fprintf(stderr,"ERROR: write()\n");
        return -1;
    }
    myDB->size --;
    dbmetadata_disk_write(myDB, -1);
    return 0;
}

// ----------------WORK WITH DB ---------------------

int dbclose(struct DB *db){
    int res;
    struct MyDB *myDB = (struct MyDB *) db;
    if(close(myDB->id_file)<0){
        fprintf(stderr,"ERROR: CANNOT close file\n");
        res = -1;
    }
    if(myDB==NULL) return -1;
    node_free(myDB,myDB->root);
    free(myDB->buffer);
    free(myDB->exist);
    free(myDB);
    return res;  
}

struct DB *dbcreate(const char *file, struct DBC conf){
    int id_file = open(file,  O_CREAT |O_RDWR | O_TRUNC, S_IRWXU );
    if(id_file<0){
        fprintf(stderr,"ERROR: NOT VALID FILE DESCRIPTOR\n");
        return NULL;
    }
    struct MyDB *myDB = (struct MyDB *) malloc(sizeof(struct MyDB)); 
    if(myDB==NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB\n");
        return NULL;
    }
    myDB->close = &dbclose;
    myDB->get = &search;
    myDB->put = &insert;
    myDB->del = &delete;
    //myDB->sync = &dbsync;
    
    myDB->root = NULL;
    myDB->buffer = NULL;
    myDB->exist = NULL;
    myDB->depth = 0;
    myDB->id_file = id_file;
    
    myDB->chunk_size = conf.chunk_size;
    
    int max_node_keys = (myDB->chunk_size - 3*sizeof(size_t)-sizeof(bool))/(3*sizeof(size_t) + MAX_KEY_LENGTH + MAX_VALUE_LENGTH); // max_node_keys == 2t-1
    if(max_node_keys % 2 == 0) max_node_keys -= 1;
    myDB->t = (max_node_keys+1)/2;
    
    myDB->size = 0;
    myDB->max_size = ((conf.db_size/myDB->chunk_size - 1 )/((double)(myDB->chunk_size+1)) ) * (myDB->chunk_size);
    
    myDB->buffer = calloc(myDB->chunk_size, sizeof(byte));
    if(myDB->buffer == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->buffer\n");
        return NULL;
    }
    size_t exist_size = (myDB->max_size/8 < myDB->chunk_size) ? myDB->chunk_size : myDB->max_size/8;
    myDB->exist =(byte *) calloc( exist_size, sizeof(byte));
    if(myDB->exist == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->exist\n");
        return NULL;
    }
    long i;
    for(i=0; i<myDB->max_size/8; i++)
        myDB->exist[i] = 0;
       
    myDB->root = create_BTreeNode(myDB);//creation of root //myDB->root->leaf = true  and  myDB->offset = 0 by default in node_malloc() 
    
    //node metadata = sizeof(offset+leaf+n) + (n+1)*sizeof(child) + n*sizeof(k+v+!!!!!!!!!16!!!!!!!!!!)
    
    //DB_METADATA = 1 chunk () + max_size/8 chunks of (exist[])
    i=0;
    memcpy((myDB->buffer+i),&(myDB->chunk_size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->t),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->max_size),sizeof(size_t));
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->root->offset),sizeof(size_t));//offset ot root
    i+=sizeof(size_t);
    memcpy((myDB->buffer+i),&(myDB->depth),sizeof(int));
    i+=sizeof(int);
    
    
    if(write(id_file, myDB->buffer , myDB->chunk_size) != myDB->chunk_size){//first main chunk with metadata in file
        fprintf(stderr,"ERROR: write\n");
        return NULL;
    } 
    //next in file (max_size/8)*chunk_size pages of bitset myDB->exist
    memset(myDB->buffer,0,myDB->chunk_size*sizeof(byte));
    long j;
    for(j=0; j < (myDB->max_size / 8) / myDB->chunk_size; j++){
        if(write(id_file, myDB->buffer,myDB->chunk_size) != myDB->chunk_size){
            fprintf(stderr,"ERROR: write\n");
            return NULL;
        }
    }
    node_disk_write(myDB,myDB->root);
    
    return (struct DB *)myDB;
}

struct DB *dbopen(const char *file){
    int id_file = open(file, O_RDWR, S_IRWXU);
    if(id_file<0){
        fprintf(stderr,"ERROR: NOT VALID FILE DESCRIPTOR in dbopen\n");
        return NULL;
    }
    struct MyDB *myDB = (struct MyDB *) malloc(sizeof(struct MyDB));
    if(myDB==NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB\n");
        return NULL;
    }
    myDB->root = NULL;
    myDB->buffer = NULL;
    myDB->exist = NULL;
    myDB->id_file = id_file;
    
    if(read(id_file,&(myDB->chunk_size) ,sizeof(size_t)) != sizeof(size_t)){
        fprintf(stderr,"ERROR: CANNOT read chunk_size\n");
        return NULL;
    }
    
    myDB->buffer = (byte *) malloc(myDB->chunk_size*sizeof(byte)); 
    if(myDB->buffer == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->buffer\n");
        return NULL;
    }
    
    if(lseek(id_file, 0, SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return NULL;
    }
    
    if(read(id_file, myDB->buffer ,myDB->chunk_size) != myDB->chunk_size){
        fprintf(stderr,"ERROR: CANNOT read\n");
        return NULL;
    }
    
    
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
    memcpy(&(myDB->depth),(myDB->buffer+i),sizeof(int));
    i+=sizeof(int);
    
    myDB->root = node_disk_read(myDB,root_offset);
    
    myDB->exist = calloc(myDB->max_size/8 , sizeof(byte));
    if(myDB->exist == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->exist\n");
        return NULL;
    }
    
    if(lseek(id_file, myDB->chunk_size, SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return NULL;
    }
    long j;
    //next in file (max_size/8)/chunk_size pages of bitset myDB->exist
    for(j=0;j < (myDB->max_size / 8)/ myDB->chunk_size; j++){
        if(read(id_file, myDB->buffer,myDB->chunk_size)!=myDB->chunk_size){
            fprintf(stderr,"ERROR: read()\n");
            return NULL;
        }
        for(i=0;i<myDB->chunk_size;i++)
            myDB->exist[i+j*myDB->chunk_size] = myDB->buffer[i];
    }
    return (struct DB *)myDB;
}


// -------------------- BTREE ALGORITHMS --------------


int compare(const struct DBT k1, const struct DBT k2){
    //return memcmp(k1.data, k2.data, k1.size);
    // version for key = string, representing number. example: "12", "145". P.S."00002323" == "2323", "99" < "100", "99" > "0999"
    if(k1.size == k2.size){
        return strncmp(k1.data, k2.data, k1.size);
    }
    else if(k2.size == 0 || k1.size==0) {
        fprintf(stderr,"ERROR: compare: k1 = %s, k1size:%lu, k2 = %s, k2size:%lu\n",(char *)k1.data, k1.size, (char *)k2.data, k2.size );
        if (k1.size > k2.size) return 1;
        else if(k1.size < k2.size) return -1;
        else return 0;
    }
    else if(k1.size > k2.size){
        size_t diff = k1.size - k2.size;
        char *r2 = calloc(k1.size+1, sizeof(char));
        if(r2==NULL) {
            fprintf(stderr,"ERROR: malloc in compare\n");
            return 1;
        }
        r2[k1.size-1] = '\0';
        long i;
        for(i=0;i<diff;i++)
            r2[i]='0';
        r2[i]='\0';    
        strncat(r2,k2.data,k2.size-1);
        int result = strncmp(k1.data, r2, k1.size);
        free(r2);
        return result;
    }
    else {
        size_t diff = k2.size - k1.size;
        char *r1 = calloc(k2.size+1, sizeof(char));
        if(r1==NULL) {
            fprintf(stderr,"ERROR: malloc in compare\n");
            return -1;
        }
        r1[k2.size-1] = '\0';
        long i;
        for(i=0;i<diff;i++)
            r1[i]='0';
        r1[i]='\0';    
        strncat(r1,k1.data,k1.size-1);
        int result = strncmp(r1,k2.data, k2.size);
        free(r1);
        return result;
    }
}

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

//-------------- Delete ---------------
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
        return -1;
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
                    return -1;
                }
                node_free(myDB,b);
            }
            node_free(myDB,a);
            return res;
        }
    }
    else{
        if(x->leaf) {
            return -1;
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
            return -1;
        }
    }
}

int delete(struct DB *db, struct DBT * key){
    struct MyDB * myDB = (struct MyDB *) db;
    if(myDB->root->n == 0) fprintf(stderr,"DB is already empty\n");
    delete_from_node(myDB, myDB->root, key);
    return 0;
}



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
//interface

int db_close(struct DB *db) {
	return db->close(db);
}

int db_del(struct DB *db, void *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->del(db, &keyt);
}

int db_get(struct DB *db, void *key, size_t key_len,
	   void **val, size_t *val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {0, 0};
	int rc = db->get(db, &keyt, &valt);
	*val = valt.data;
	*val_len = valt.size;
	return rc;
}

int db_put(struct DB *db, void *key, size_t key_len,
	   void *val, size_t val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {
		.data = val,
		.size = val_len
	};
	return db->put(db, &keyt, &valt);
}



// --------------------- Main OPEN AND CREATE for debug purposes ---------------

int main1(){
    struct DB *db = dbopen("my.db");
    
    /*
    struct DBT key,value;
    value.data = NULL;
    key.data = (char *)malloc(MAX_KEY_LENGTH);
    char s[MAX_KEY_LENGTH+1]="456870127881611797542";
    key.size = 1+strlen(s);
    memcpy(key.data, s, key.size);
    if(search(db,&key,&value)<0) fprintf(stderr,"(____|____) NOT FOUND!!!\n");
    else fprintf(stderr,"KEY = %s FOUND %s\n",(char *)key.data, (char *)value.data);
    delete(db,&key);
    if(search(db,&key,&value)<0) fprintf(stderr,"(____|____) NOT FOUND!!!\n");
    else fprintf(stderr,"KEY = %s FOUND %s\n",(char *)key.data, (char *)value.data);
    
    if(value.data!=NULL) free(value.data);
    free(key.data);
    */
    print_DB_info(db);
    dbclose(db);
    return 0;
}

int main(int argc, char *argv[]){
    if(argc > 1 && strcmp(argv[1],"open")==0){
        main1();
        return 0;
    }

    struct DBC dbc;
    dbc.db_size = 512 * 1024 * 1024;
    dbc.chunk_size = 4096;
    //dbc.mem_size = 16 * 1024 * 1024;
    struct DB *db = dbcreate("my.db",dbc); 
    const int n = 10000;
    struct DBT key[n],value[n];
    int j=0,i=0;
    
    for(i=0;i<n;i++){
        key[i].data = malloc(MAX_KEY_LENGTH);
        value[i].data = malloc(MAX_VALUE_LENGTH);
        key[i].size = MAX_KEY_LENGTH;
        value[i].size = MAX_VALUE_LENGTH;
    }

    char kbuf[MAX_KEY_LENGTH];
    char vbuf[MAX_VALUE_LENGTH];
    memset(kbuf,0,MAX_KEY_LENGTH);
    memset(vbuf,0,MAX_VALUE_LENGTH);
    
    srand(time(NULL));
    for(j=0;j<n;j++) {
        for(i=0;i<key[j].size-1;i++){
            kbuf[i] = '0' + rand() % 10;
        }
        for(i=0;i<value[j].size-1;i++){
            vbuf[i] = 'a' + rand() % 26;
        }
        memcpy(key[j].data, kbuf, key[j].size);
        memcpy(value[j].data, vbuf, value[j].size);
    }
    
    for(j=0;j<n;j++){
        insert(db,&key[j],&value[j]);
    }
    
    print_DB_info(db);
    
    int nk = get_current_n(db);
    
    for (j = 0; j < n; j++){
        if (delete(db,&key[j]) == -1) fprintf(stderr, "Delete -1\n");
        nk--;
        if(get_current_n(db)!=nk) {
            fprintf(stderr, "SOMETHING GO WRONG IN DELETE! %d ITERATION, %d %d . current key %s\n", j,nk,get_current_n(db),(char *)key[j].data);
        }
    }
    
    for(j=0;j<n;j++){
        free(key[j].data);
        free(value[j].data);
    }
    
    dbclose(db);
    return 0;
}

