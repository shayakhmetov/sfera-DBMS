#include "work_with_key.h"

void copy_key(struct BTreeNode *x, long i, struct BTreeNode *y, long j ){
    memcpy(x->keys[i].data, y->keys[j].data, MAX_KEY_LENGTH);
    x->keys[i].size = y->keys[j].size;
    memcpy(x->values[i].data, y->values[j].data, MAX_VALUE_LENGTH);
    x->values[i].size = y->values[j].size;
}

int compare(const struct DBT k1, const struct DBT k2){
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

