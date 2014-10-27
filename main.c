#include "db.h"

#ifndef __MYDB__DELETE__
    #include "delete.h"
#endif
#ifndef __MYDB__INSERT__
    #include "insert.h"
#endif
#ifndef __MYDB__SEARCH__
    #include "search.h"
#endif
#ifndef __MYDB__WORK_WITH_DISK__
    #include "work_with_disk.h"
#endif
#ifndef __MYDB__WORK_WITH_KEY__
    #include "work_with_key.h"
#endif
#ifndef __MYDB__PRINT_FOR_DEBUG__
    #include "print_for_debug.h"
#endif
#ifndef __MYDB__NODE_ALLOC__
    #include "node_alloc.h"
#endif

#include <time.h>

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

