#include "work_with_disk.h"

#include "delete.h"
#include "insert.h"
#include "search.h"
#include "node_alloc.h"
#include "cache.h"
// ------------------ DISK OPERATIONS ----------------- 
// offset(number of node) to real offset in file
size_t convert_offset(struct MyDB *myDB, size_t offset){
    size_t temp = ((myDB->max_size/8) < myDB->chunk_size) ? 1 : (myDB->max_size/8)/myDB->chunk_size;
    return (offset + 1 + temp) * myDB->chunk_size; //+1 is because DB metadata in first chunk, next chunks for bitset
}

int node_flush(struct MyDB *myDB, struct BTreeNode *node){
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

int node_disk_write(struct MyDB *myDB, struct BTreeNode *node){
    #ifndef __CACHE__
    return node_flush(myDB, node);//without cache
    #endif
    if(node!=myDB->root){
        cache_write_node(myDB, node);
    }
    return node_flush(myDB, node);
}

struct BTreeNode *node_load(struct MyDB *myDB, size_t offset){
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

struct BTreeNode *node_disk_read(struct MyDB *myDB, size_t offset ){
    #ifndef __CACHE__
    return node_load(myDB, offset);//without cache
    #endif
    struct BTreeNode *node = cache_find_node(myDB, (int)offset);
    if(node == NULL){
        node = node_load(myDB, offset);
        cache_add_node(myDB, node);
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
    #ifdef __CACHE__
    cache_delete_node(myDB, (int)offset);
    #endif
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
    #ifdef __CACHE__
    cache_free(myDB);
    #endif
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
    myDB->cache_size = conf.mem_size/conf.chunk_size;
    
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
    memcpy((myDB->buffer+i),&(myDB->cache_size),sizeof(size_t));
    i+=sizeof(size_t);
    
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
    node_flush(myDB,myDB->root);
    
    #ifdef __CACHE__
    myDB->cache = cache_create(myDB->cache_size);
    #endif
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
    memcpy(&(myDB->cache_size),(myDB->buffer+i),sizeof(size_t));
    i+=sizeof(size_t);
    
    myDB->root = node_load(myDB,root_offset);
    
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
    #ifdef __CACHE__
    myDB->cache = cache_create(myDB->cache_size);
    #endif
    return (struct DB *)myDB;
}
