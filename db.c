#include "db.h" 
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include <time.h>

#define MAX_KEY_LENGTH  16
#define MAX_VALUE_LENGTH  64

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


// offset(number of node) to real offset in file
size_t convert_offset(const struct MyDB *myDB, size_t offset){
    return (offset + 1 + myDB->max_size/myDB->chunk_size ) * myDB->chunk_size; //+1 is because DB metadata in first chunk, next chunks for bitmask
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
    if(file_id<0){
        fprintf(stderr,"ERROR: NOT VALID FILE DESCRIPTOR\n");
        return -1;
    }
    if(lseek(file_id, convert_offset(myDB, node->offset), SEEK_SET)<0){
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
    
    if(write(file_id, myDB->buffer , myDB->chunk_size) != myDB->chunk_size){
        fprintf(stderr,"ERROR: write()\n");
        return -1;
    }
    return 0;
}


struct BTreeNode *node_disk_read(const struct MyDB *myDB, size_t offset ){
    int file_id = myDB->id_file;
    if(file_id<0){
        fprintf(stderr,"NOT VALID FILE DESCRIPTOR\n");
        return NULL;
    }
    struct BTreeNode *node = node_malloc(myDB);
    
    if(lseek(file_id, convert_offset(myDB, offset), SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return NULL;
    }
    long i=0;
    if(read(file_id, myDB->buffer, myDB->chunk_size) != myDB->chunk_size){
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
    
    if(lseek(myDB->id_file, 0, SEEK_SET) < 0){
        fprintf(stderr,"ERROR: lseek()\n");
        return -1;
    }
    if(write(myDB->id_file, myDB->buffer , myDB->chunk_size) != myDB->chunk_size){ //first main chunk with metadata in file
        fprintf(stderr,"ERROR: write()\n");
        return -1;
    }
    if(index>=0 && index<myDB->max_size){
        //next in file (max_size/chunk_size) pages of bitmask myDB->exist
        long j = index / myDB->chunk_size;
        for(i=0; i < myDB->chunk_size; i++)
            myDB->buffer[i] = myDB->exist[i+j*myDB->chunk_size];
        if(lseek(myDB->id_file, j*myDB->chunk_size, SEEK_CUR) < 0){
            fprintf(stderr,"ERROR: lseek()\n");
            return -1;
        }
        if(write(myDB->id_file, myDB->buffer,myDB->chunk_size) != myDB->chunk_size ){
            fprintf(stderr,"ERROR: write()\n");
            return -1;
        }
    }
    return 0;
}


// ----------------WORK WITH DB ---------------------

struct DB *dbcreate(const char *file, const struct DBC *conf){
    if(unlink(file)<0){
        fprintf(stderr,"ERROR: unlink()\n");
        return NULL;
    }
    int file_id = open(file,  O_CREAT |O_RDWR | O_TRUNC, S_IRWXU );
    if(file_id<0){
        fprintf(stderr,"ERROR: NOT VALID FILE DESCRIPTOR\n");
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
    myDB->id_file = file_id;
    
    myDB->chunk_size = conf->chunk_size;
    int max_node_keys = (myDB->chunk_size - 3*sizeof(size_t)-sizeof(bool))/(3*sizeof(size_t) + MAX_KEY_LENGTH + MAX_VALUE_LENGTH); // max_node_keys == 2t-1
    if(max_node_keys % 2 == 0) max_node_keys -= 1;
    myDB->t = (max_node_keys+1)/2;
    myDB->size = 0;
    myDB->max_size = ( (conf->db_size/myDB->chunk_size-1)/(myDB->chunk_size+1) ) * myDB->chunk_size;//first chunk - DB metadata
    myDB->buffer = calloc(myDB->chunk_size, sizeof(char));
    if(myDB->buffer == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->buffer\n");
        return NULL;
    }
    myDB->exist =(bool *) calloc(myDB->max_size, sizeof(bool));
    if(myDB->exist == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->exist\n");
        return NULL;
    }
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
    
    if(write(file_id, myDB->buffer , myDB->chunk_size) != myDB->chunk_size){//first main chunk with metadata in file
        fprintf(stderr,"ERROR: write\n");
        return NULL;
    } 
    //next in file (max_size/chunk_size) pages of bitmask myDB->exist
    memset(myDB->buffer,0,myDB->chunk_size*sizeof(char));
    long j=0;
    for(j=0; j < myDB->max_size / myDB->chunk_size; j++)
        if(write(file_id, myDB->buffer,myDB->chunk_size) != myDB->chunk_size){
            fprintf(stderr,"ERROR: write\n");
            return NULL;
        }
        
    node_disk_write(myDB,myDB->root);
    return (struct DB *)myDB;
}

struct DB *dbopen(const char *file){
    int file_id = open(file, O_RDWR, S_IRWXU);
    if(file_id<0){
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
    myDB->id_file = file_id;
    
    if(read(file_id,&(myDB->chunk_size) ,sizeof(size_t)) != sizeof(size_t)){
        fprintf(stderr,"ERROR: CANNOT read chunk_size\n");
        return NULL;
    }
    
    myDB->buffer = (char *) malloc(myDB->chunk_size*sizeof(char)); 
    if(myDB->buffer == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->buffer\n");
        return NULL;
    }
    
    if(lseek(file_id, 0, SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return NULL;
    }
    
    if(read(file_id, myDB->buffer ,myDB->chunk_size) != myDB->chunk_size){
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
    
    myDB->root = node_disk_read(myDB,root_offset);
    //print_Node_info((struct DB *)myDB,myDB->root);
    myDB->exist = calloc(myDB->max_size , sizeof(bool));
    if(myDB->exist == NULL){
        fprintf(stderr,"ERROR: CANNOT MALLOC myDB->exist\n");
        return NULL;
    }
    
    if(lseek(file_id, myDB->chunk_size, SEEK_SET)<0){
        fprintf(stderr,"ERROR: lseek()\n");
        return NULL;
    }
    long j;
    //next in file (max_size/chunk_size) pages of bitmask myDB->exist
    for(j=0;j < myDB->max_size / myDB->chunk_size; j++){
        if(read(file_id, myDB->buffer,myDB->chunk_size)!=myDB->chunk_size){
            fprintf(stderr,"ERROR: read()\n");
            return NULL;
        }
        for(i=0;i<myDB->chunk_size;i++)
            myDB->exist[i+j*myDB->chunk_size] = myDB->buffer[i];
    }
    //print_DB_info((struct DB *)myDB);
    return (struct DB *)myDB;
}

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


// -------------------- BTREE ALGORITHMS --------------


int compare(const struct DBT k1, const struct DBT k2){// key = string, representing number. example: "12", "145". P.S."00002323" == "2323", "99" < "100", "99" > "0999"
    if(k1.size == k2.size){
        return strncmp(k1.data, k2.data, k1.size);
    }
    else if(k2.size == 0 || k1.size==0) {
        fprintf(stderr,"ERROR: compare WTF???\n");
        return 0;
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

struct BTreeNode *split_child(struct MyDB *myDB, struct BTreeNode *x,long i){//returns new created node
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

    for(j=x->n; j>i; j--)
        x->childs[j+1]= x->childs[j];
    x->childs[i+1] = z->offset;
    for(j=x->n; j>i; j--){
        memcpy(x->keys[j].data,x->keys[j-1].data, MAX_KEY_LENGTH);
        x->keys[j].size = x->keys[j-1].size;
        memcpy(x->values[j].data,x->values[j-1].data, MAX_VALUE_LENGTH);
        x->values[j].size = x->values[j-1].size;
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
    return z;
}


int insert_nonfull(struct MyDB *myDB, struct BTreeNode *x, struct DBT *key, struct DBT *data){
    long i = x->n-1;
    if(x->leaf){
       while(i>=0 && compare(*key , x->keys[i])<0 ){
            memcpy(x->keys[i+1].data,x->keys[i].data, MAX_KEY_LENGTH);
            memcpy(&(x->keys[i+1].size), &(x->keys[i].size),sizeof(size_t));
            memcpy(x->values[i+1].data,x->values[i].data, MAX_VALUE_LENGTH);
            memcpy(&(x->values[i+1].size), &(x->values[i].size),sizeof(size_t));
            i--;
        }
        i++;
        memcpy(x->keys[i].data, key->data, (key->size < MAX_KEY_LENGTH) ? key->size : MAX_KEY_LENGTH);
        memcpy(&(x->keys[i].size), &(key->size),sizeof(size_t));
        memcpy(x->values[i].data,data->data, (data->size < MAX_VALUE_LENGTH) ? data->size : MAX_VALUE_LENGTH);
        memcpy(&(x->values[i].size), &(data->size),sizeof(size_t));
        
        x->n ++;
        node_disk_write(myDB,x);
    }
    else{
        while(i>=0 && compare(*key , x->keys[i])<0)
            i--;
        i++;
        bool succ_child = false;        
        struct BTreeNode *c = node_disk_read(myDB, x->childs[i]); 
        struct BTreeNode *z = NULL;
        if(c->n == 2*(myDB->t)-1){
            z = split_child(myDB,x,i);
            if(compare(*key, x->keys[i])>0) succ_child = true;
        }
        if(succ_child == true) {
            insert_nonfull(myDB, z, key, data);
        }
        else{
            node_free(myDB,c);
            c = node_disk_read(myDB, x->childs[i]);
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
        myDB->root = s;
        s->childs[0] = r->offset;
        //s->n = 0; by default in node_malloc
        dbmetadata_disk_write(myDB,-1);
        node_disk_write(myDB,r);
        node_disk_write(myDB,s);
        node_free(myDB,r);
        
        struct BTreeNode *z = split_child(myDB,s,0);
        node_free(myDB,z);
        
        insert_nonfull(myDB, s, key, data);
    }
    else {
        insert_nonfull(myDB, r, key ,data);
    }
    return 0;
}


struct BTreeSearchResult *search_recursive(const struct MyDB *myDB, struct BTreeNode *x, struct DBT *key){
    long i=0;
    while(i<=x->n-1 && compare(*key, x->keys[i]) > 0)
        i++;
    if(i<=x->n-1 && compare(*key, x->keys[i]) == 0){
        struct BTreeSearchResult *res = malloc(sizeof(struct BTreeSearchResult));
        res->node = x;
        res->index = i;
        return res;
    }
    else if(x->leaf){
        if(myDB->root != x) node_free(myDB,x);
        return NULL;
    }
    else{
        struct BTreeNode *c = node_disk_read(myDB, x->childs[i]);
        struct BTreeSearchResult *res = search_recursive(myDB, c , key);
        if(res!=NULL && res->node != c) node_free(myDB,c);  
        return res;
    }
}


int search(const struct DB *db, struct DBT *key, struct DBT *data){
    const struct MyDB *myDB = (const struct MyDB *)db;
    struct BTreeSearchResult *res = search_recursive(myDB, myDB->root, key);
    if(res == NULL) return -1;
    memcpy(data->data, res->node->values[res->index].data, res->node->values[res->index].size);
    memcpy(&(data->size), &(res->node->values[res->index].size), sizeof(size_t));
    if(res->node != myDB->root && res->node!=NULL) node_free(myDB,res->node);
    free(res); 
    return 0;
}


// --------------------- print for debug purposes ----------------

void print_DB_info(const struct DB *db){
    const struct MyDB *myDB = (const struct MyDB *) db; 
    printf("\nDATABASE INFO\n");
    printf("t -> %lu \n",myDB->t);
    printf("chunk_size -> %lu \n",myDB->chunk_size);
    printf("max_size -> %lu \n",myDB->max_size);
    printf("size -> %lu \n",myDB->size);
    long i;
    printf("-------EXISTS:\n");
    for(i=0;i<myDB->size;i++){
        printf("%d",myDB->exist[i]);
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
    printf("\n-----------------CHILDREN of %lu\n\n",node->offset);   
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


// --------------------- Main OPEN AND CREATE for debug purposes ---------------

int main1(){
    struct DB *db = dbopen("temp.db");
    //print_DB_info(db);
    
    struct DBT key,value;
    
    key.data = (char *)calloc(MAX_KEY_LENGTH, sizeof(char));
    value.data = (char *)calloc(MAX_VALUE_LENGTH, sizeof(char));
    key.size = MAX_KEY_LENGTH;
    char kbuf[MAX_KEY_LENGTH];
    kbuf[MAX_KEY_LENGTH-1] = '\0';
    
    key.size=3;
    kbuf[0]='1';
    kbuf[1]='2';
    kbuf[2]='\0';
    memcpy(key.data, kbuf, key.size);
    
    if(search(db,&key,&value)<0) fprintf(stderr,"(~____~)");
    else fprintf(stderr,"%s\n",(char *)value.data);
    
    dbclose(db);
    return 0;
}

int main(){
    main1();
    return 0;
    
    struct DBC dbc;
    dbc.db_size = 512 * 1024 * 1024;
    dbc.chunk_size = 4096;
    dbc.mem_size = 16 * 1024 * 1024;
    struct DB *db = dbcreate("temp.db",&dbc); //dbopen or dbcreate
    struct DBT key,value;
    print_DB_info(db);
    key.data = malloc(MAX_KEY_LENGTH);
    value.data = malloc(MAX_VALUE_LENGTH);
    int j=0,i=0;
    
    char kbuf[MAX_KEY_LENGTH];
    char vbuf[MAX_VALUE_LENGTH];
    memset(kbuf,0,MAX_KEY_LENGTH);
    memset(vbuf,0,MAX_VALUE_LENGTH);
    kbuf[MAX_KEY_LENGTH-1] = '\0';
    vbuf[MAX_VALUE_LENGTH-1] = '\0';
    key.size = MAX_KEY_LENGTH;
    value.size = MAX_VALUE_LENGTH;
    
    printf("\nAFTER INSERT:\n\n");
    srand(time(NULL));
    for(j=0;j<10000;j++) {// (keys,values)

        for(i=0;i<key.size-1;i++){
            kbuf[i] = '0' + rand() % 10;
        }
        for(i=0;i<value.size-1;i++){
            vbuf[i] = 'a' + rand() % 26;
        }
        memcpy(key.data, kbuf, key.size);
        memcpy(value.data, vbuf, value.size);
        insert(db,&key,&value);
    }
    fprintf(stderr,"KEY %s\n",(char *)key.data);
    if(search(db,&key,&value)<0) fprintf(stderr,"(____|____) NOT FOUND!!!\n");
    else fprintf(stderr,"SEARCHED %s\n",(char *)value.data);
    
    
    key.size=3;
    kbuf[0]='1';
    kbuf[1]='2';
    kbuf[2]='\0';
    memcpy(key.data, kbuf, key.size);
    insert(db,&key,&value);
    
    
    print_DB_info(db);
    dbclose(db);
    return 0;
}
