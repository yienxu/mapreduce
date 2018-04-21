#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mapreduce.h"

#define INIT_SIZE (15)
#define INIT_ARRLIST_SIZE (10)

Partitioner partition_func;
static int num_partitions;

//define node and hashtable
typedef struct {
    long size;
    long num_items;
    char **val_list;
    pthread_mutex_t lock;
} ArrList;

typedef struct {
    ArrList *list;
    char *key;
} Element;

typedef struct {
    int size;
    int count;
    Element **elements;
    pthread_mutex_t lock;
} ht_table;

void init_list(ArrList *arrList) {
    pthread_mutex_init(&arrList->lock, NULL);
    arrList->size = INIT_ARRLIST_SIZE;
    arrList->num_items = 0;
    arrList->val_list = malloc(arrList->size * sizeof(char *));
}

void list_add(ArrList *arrList, char *val) {
    pthread_mutex_lock(&arrList->lock);
    if (arrList->num_items == arrList->size) {
        arrList->size *= 2;
        arrList->val_list = realloc(arrList->val_list, arrList->size);
    }
    arrList->val_list[arrList->num_items++] = val;
    pthread_mutex_unlock(&arrList->lock);
}

//find the next prime to be used as hashtable resizing 
int is_prime(const int x) {
    if (x < 2) { return -1; }
    if (x < 4) { return 1; }
    if ((x % 2) == 0) { return 0; }
    for (int i = 3; i <= floor(sqrt((double) x)); i += 2) {
        if ((x % i) == 0) {
            return 0;
        }
    }
    return 1;
}

int next_prime(int x) {
    while (is_prime(x) != 1) {
        x++;
    }
    return x;
}

//initialize new hashtable
ht_table *new_table(int size) {
    ht_table *table = malloc(sizeof(ht_table));
    table->size = next_prime(size);
    table->count = 0;
    table->elements = calloc(table->size, sizeof(Element *));
    pthread_mutex_init(&table->lock, NULL);
    return table;
}

Element **new_element_list(int size) {
    return calloc(size, sizeof(Element *));
}

static ht_table **tables;

//hash function
int ht_hash(const char *s, const int a, const int m) {
    long hash = 0;
    const int len_s = strlen(s);
    for (int i = 0; i < len_s; i++) {
        hash += (long) pow(a, len_s - (i + 1)) * s[i];
        hash = hash % m;
    }
    return (int) hash;
}

int ht_get_hash(const char *s, const int num_buckets, const int attempt) {
    int prime1 = next_prime(strlen(s));
    int prime2 = next_prime(prime1);
    const int hash_a = ht_hash(s, prime1, num_buckets);
    const int hash_b = ht_hash(s, prime2, num_buckets);
    return (hash_a + (attempt * (hash_b + 1))) % num_buckets;
}

void ht_free(ht_table *table) {
    int i, j;
    for (i = 0; i < table->size; i++) {
        Element *element = table->elements[i];
        if (element == NULL) {
            continue;
        }
        free(element->key);
        for (j = 0; j < element->list->num_items; j++) {
            free(element->list->val_list[j]);
        }
        free(element->list);
        free(element);
    }
    free(table);
}

void expand(ht_table *table) {
    int old_size = table->size;
    int new_size = next_prime(old_size * 2);
    Element **old_elements = table->elements;
    table->elements = new_element_list(new_size);
    table->size = new_size;
    int i;
    for (i = 0; i < old_size; i++) {
        Element *element = old_elements[i];
        if (element == NULL) {
            continue;
        }
        int attempt = 0;
        while (1) {
            int hash_index = ht_get_hash(element->key, table->size, attempt++);
            if (table->elements[hash_index] == NULL) {
                table->elements[hash_index] = element;
                break;
            }
        }
    }
}

void ht_insert(ht_table *table, char *key, char *value) {
    int load = table->count * 100 / table->size;
    if (load > 70) {
        expand(table);
    }

    int attempt = 0;
    int index;
    while (1) {
        index = ht_get_hash(key, table->size, attempt++);
        Element *element = table->elements[index];
        pthread_mutex_lock(&table->lock);
        if (element == NULL) {
            element = malloc(sizeof(Element));
            table->elements[index] = element;
            pthread_mutex_unlock(&table->lock);
            element->key = strdup(key);
//            printf("inserting: %p into %p\n", element->key, element);
            element->list = malloc(sizeof(ArrList));
            init_list(element->list);
            list_add(element->list, strdup(value));
//            printf("NULL finished\n"); fflush(stdout);
            table->count++;
            break;
        } else {
            pthread_mutex_unlock(&table->lock);
//            printf("element->key: %s\n", element->key);
            if (strcmp(key, element->key) == 0) {
                list_add(element->list, strdup(value));
                break;
            } else {
                index = ht_get_hash(key, table->size, attempt++);
            }
        }
    }
    printf("insert (%s, %s)\n", key, value); fflush(stdout);
}

void MR_Emit(char *key, char *value) {
    int part = partition_func(key, num_partitions);
    ht_insert(tables[part], key, value);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

static Mapper mapper;
static int argcnt;
static char **argvec;
static int ptr = 1;

pthread_mutex_t filelock = PTHREAD_MUTEX_INITIALIZER;

char *get_filename() {
    pthread_mutex_lock(&filelock);
    char *filename;
    if (ptr >= argcnt) {
        filename = NULL;
    } else {
        filename = argvec[ptr++];
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

int compar(const void *e1, const void *e2) {
    Element *a = *(Element **)e1;
    Element *b = *(Element **)e2;
    if (a == NULL && b == NULL) {
        return 0;
    }
    if (a != NULL && b != NULL && a->key == NULL && b->key == NULL) {
        return 0;
    }
    if (a == NULL || a->key == NULL) {
        return 1;
    } else if (b == NULL || b->key == NULL) {
        return -1;
    }
    return strcmp(a->key, b->key);
}

void *sort_thread(void *arg) {
    int index = *(int *)arg;
    ht_table *table = tables[index];
    Element **elements = table->elements;
    printf("before qsort %d\n", index); fflush(stdout);
    qsort(elements, table->size, sizeof(Element *), compar);
    printf("after qsort %d\n", index); fflush(stdout);
    free(arg);
    return NULL;
}

char *get_next(char *key, int partition_num) {
    Element **elements = tables[partition_num]->elements;
    return NULL;
}

void *reduce_thread(void *arg) {
    return NULL;
}

void
MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {
    partition_func = partition;
    num_partitions = num_reducers;
    mapper = map;
    argcnt = argc;
    argvec = argv;

    tables = malloc(num_reducers * sizeof(ht_table));
    int i;
    for (i = 0; i < num_reducers; i++) {
        tables[i] = new_table(INIT_SIZE);
    }

    pthread_t mthreads[num_mappers];
    for (i = 0; i < num_mappers; i++) {
        pthread_create(&mthreads[i], NULL, map_thread, NULL);
    }
    for (i = 0; i < num_mappers; i++) {
        pthread_join(mthreads[i], NULL);
    }

    pthread_t sthreads[num_partitions];
    for (i = 0; i < num_partitions; i++) {
        int *index = malloc(sizeof(int));
        *index = i;
        pthread_create(&sthreads[i], NULL, sort_thread, index);
    }
    for (i = 0; i < num_partitions; i++) {
        pthread_join(sthreads[i], NULL);
    }


    for (int ind = 0; ind < num_reducers; ind++) {
        ht_table *table = tables[ind];
        printf("\n\nStart printing...\n\n"); fflush(stdout);
        Element *element;
        for (i = 0; i < table->size; i++) {
            printf("element %d: %p\n", i, table->elements[i]);
            if (table->elements[i] != NULL) {
                element = (table->elements)[i];
                printf("%s->", element->key);
                int j;
                for (j = 0; j < element->list->num_items; j++) {
                    printf("%s, ", element->list->val_list[j]);
                }
                printf("\n");
            }
        }
        ht_free(table);
    }


    free(tables);
}
