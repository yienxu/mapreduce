#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mapreduce.h"

#define INIT_ARRLIST_SIZE (65536)

typedef struct {
    char *key;
    char *val;
} Element;

typedef struct {
    int capacity;
    int size;
    Element *elements;
    pthread_mutex_t lock;
} ArrList;

Partitioner partition_func;
Mapper mapper;

ArrList **lists;

int num_partitions;
int argcnt;
char **argvec;
int fileptr;

pthread_mutex_t filelock = PTHREAD_MUTEX_INITIALIZER;


void init_list(ArrList *arrList) {
    pthread_mutex_init(&arrList->lock, NULL);
    arrList->capacity = INIT_ARRLIST_SIZE;
    arrList->size = 0;
    arrList->elements = malloc(arrList->capacity * sizeof(Element));
}

void list_add(ArrList *arrList, Element e) {
    pthread_mutex_lock(&arrList->lock);
    if (arrList->size == arrList->capacity) {
        arrList->capacity *= 2;
        arrList->elements = realloc(arrList->elements, arrList->capacity*sizeof(Element));
    }
    arrList->elements[arrList->size++] = e;
    pthread_mutex_unlock(&arrList->lock);
}

void MR_Emit(char *key, char *value) {
    int part = partition_func(key, num_partitions);
    ArrList *arrList = lists[part];
    Element e;
    e.key = strdup(key);
    e.val = strdup(value);
    list_add(arrList, e);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

char *get_filename() {
    pthread_mutex_lock(&filelock);
    char *filename;
    if (fileptr >= argcnt) {
        filename = NULL;
    } else {
        filename = argvec[fileptr++];
    }
    pthread_mutex_unlock(&filelock);
    return filename;
}

void *map_thread(void *arg) {
    char *filename;
    while ((filename = get_filename()) != NULL) {
        mapper(filename);
    }
    return NULL;
}

// TODO: check if compar is correct
int compar(const void *e1, const void *e2) {
    Element a = *(Element *)e1;
    Element b = *(Element *)e2;
    return strcmp(a.key, b.key);
}

void *sort_thread(void *arg) {
    int index = *(int *)arg;
    ArrList *arrList = lists[index];
    Element *elements = arrList->elements;
    qsort(&elements[0], arrList->size, sizeof(Element), compar);
    free(arg);
    return NULL;
}

int **kptrs;

char *get_next(char *key, int partition_num) {
    Element *elements = lists[partition_num]->elements;
    int *kptr = kptrs[partition_num];
    char *val = NULL;
    if (*kptr >= lists[partition_num]->size) {
        return NULL;
    }
    if (strcmp(key, elements[*kptr].key) == 0) {
        if (*kptr < lists[partition_num]->size) {
            val = elements[(*kptr)++].val;
        }
    }
    return val;
}

typedef struct {
    Reducer reducer;
    int partition_number;
} ReArgs;

void *reduce_thread(void *arg) {
    ReArgs *reArgs = (ReArgs *)arg;

    Reducer reducer = reArgs->reducer;
    int partition_number = reArgs->partition_number;
    int *kptr = kptrs[partition_number];

    Element *elements = lists[partition_number]->elements;
    for (*kptr = 0; *kptr < lists[partition_number]->size;) {
        char *key = elements[*kptr].key;
        reducer(key, get_next, partition_number);
    }
    free(reArgs);
    return NULL;
}

void ptrs_free(int **ptrs, int size) {
    int i;
    for (i = 0; i < size; i++) {
        free(ptrs[i]);
    }
    free(ptrs);
}

void
MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {
    partition_func = partition;
    num_partitions = num_reducers;
    mapper = map;
    argcnt = argc;
    argvec = argv;
    fileptr = 1;

    // initialize hash tables
    lists = malloc(num_partitions * sizeof(ArrList *));
    int i;
    for (i = 0; i < num_partitions; i++) {
        lists[i] = malloc(sizeof(ArrList));
        init_list(lists[i]);
    }

    // mapping...
    pthread_t mthreads[num_mappers];
    for (i = 0; i < num_mappers; i++) {
        pthread_create(&mthreads[i], NULL, map_thread, NULL);
    }
    for (i = 0; i < num_mappers; i++) {
        pthread_join(mthreads[i], NULL);
    }

    // sorting...
    pthread_t sthreads[num_partitions];
    for (i = 0; i < num_partitions; i++) {
        int *index = malloc(sizeof(int));
        *index = i;
        pthread_create(&sthreads[i], NULL, sort_thread, index);
    }
    for (i = 0; i < num_partitions; i++) {
        pthread_join(sthreads[i], NULL);
    }

    // initialize pointers for reduce
    kptrs = malloc(num_reducers * sizeof(int *));
    for (i = 0; i < num_reducers; i++) {
        kptrs[i] = malloc(sizeof(int));
        *kptrs[i] = 0;
    }

    // reducing...
    pthread_t rthreads[num_reducers];
    for (i = 0; i < num_reducers; i++) {
        ReArgs *reArgs = malloc(sizeof(ReArgs));
        reArgs->reducer = reduce;
        reArgs->partition_number = i;
        pthread_create(&rthreads[i], NULL, reduce_thread, reArgs);
    }
    for (i = 0; i < num_reducers; i++) {
        pthread_join(rthreads[i], NULL);
    }

//    for (i = 0; i < num_partitions; i++) {
//        ArrList *arrList = lists[i];
//        printf("List %d:\n", i);
//        int j;
//        for (j = 0; j < arrList->size; j++) {
//            Element *elements = arrList->elements;
//            char *key = elements[j].key;
//            char *val = elements[j].val;
//            printf("(%s, %s) ", key, val);
//        }
//        printf("\n");
//    }

//    ptrs_free(kptrs, num_reducers);
//    ptrs_free(vptrs, num_reducers);
//    for (i = 0; i < num_partitions; i++) {
//        ht_free(tables[i]);
//    }
//    free(tables);
}
