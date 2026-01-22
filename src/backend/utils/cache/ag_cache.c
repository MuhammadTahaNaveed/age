/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/pg_collation.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"

#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/ag_cache.h"

typedef struct graph_name_cache_entry
{
    NameData name; /* hash key */
    graph_cache_data data;
} graph_name_cache_entry;

typedef struct graph_namespace_cache_entry
{
    Oid namespace; /* hash key */
    graph_cache_data data;
} graph_namespace_cache_entry;

typedef struct label_name_graph_cache_key
{
    NameData name;
    Oid graph;
} label_name_graph_cache_key;

typedef struct label_name_graph_cache_entry
{
    label_name_graph_cache_key key; /* hash key */
    label_cache_data data;
} label_name_graph_cache_entry;

typedef struct label_graph_oid_cache_key
{
    Oid graph;
    int32 id;
} label_graph_oid_cache_key;

typedef struct label_graph_oid_cache_entry
{
    label_graph_oid_cache_key key; /* hash key */
    label_cache_data data;
} label_graph_oid_cache_entry;

typedef struct label_relation_cache_entry
{
    Oid relation; /* hash key */
    label_cache_data data;
} label_relation_cache_entry;

typedef struct label_seq_name_graph_cache_key
{
    NameData name;
    Oid graph;
} label_seq_name_graph_cache_key;

typedef struct label_seq_name_graph_cache_entry
{
    label_seq_name_graph_cache_key key; /* hash key */
    label_cache_data data;
} label_seq_name_graph_cache_entry;

/* ag_graph.name */
static HTAB *graph_name_cache_hash = NULL;
static ScanKeyData graph_name_scan_keys[1];

/* ag_graph.namespace */
static HTAB *graph_namespace_cache_hash = NULL;
static ScanKeyData graph_namespace_scan_keys[1];

/* ag_label.name, ag_label.graph */
static HTAB *label_name_graph_cache_hash = NULL;
static ScanKeyData label_name_graph_scan_keys[2];

/* ag_label.graph, ag_label.id */
static HTAB *label_graph_oid_cache_hash = NULL;
static ScanKeyData label_graph_oid_scan_keys[2];

/* ag_label.relation */
static HTAB *label_relation_cache_hash = NULL;
static ScanKeyData label_relation_scan_keys[1];

/* ag_label.seq_name, ag_label.graph */
static HTAB *label_seq_name_graph_cache_hash = NULL;
static ScanKeyData label_seq_name_graph_scan_keys[2];

/* ag_graph_schema - per edge label: (graph, edge_label_id) -> entries */
typedef struct edge_schema_cache_key
{
    Oid graph;
    int32 edge_label_id;
} edge_schema_cache_key;

typedef struct edge_schema_cache_entry
{
    edge_schema_cache_key key;  /* hash key */
    edge_schema_cache_data data;
} edge_schema_cache_entry;

static HTAB *edge_schema_cache_hash = NULL;
static ScanKeyData edge_schema_scan_keys[2];

/* start_vertex -> end_labels cache: (graph, start_label_id) -> end_label_ids */
typedef struct vertex_labels_cache_key
{
    Oid graph;
    int32 vertex_label_id;
} vertex_labels_cache_key;

typedef struct vertex_labels_cache_entry
{
    vertex_labels_cache_key key;  /* hash key */
    vertex_edge_labels_cache_data data;
} vertex_labels_cache_entry;

static HTAB *start_vertex_end_labels_cache_hash = NULL;
static HTAB *end_vertex_start_labels_cache_hash = NULL;

/* initialize all caches */
static void initialize_caches(void);

/* common */
static void ag_cache_scan_key_init(ScanKey entry, AttrNumber attno,
                                   RegProcedure func);
static int name_hash_compare(const void *key1, const void *key2, Size keysize);

/* ag_graph */
static void initialize_graph_caches(void);
static void create_graph_caches(void);
static void create_graph_name_cache(void);
static void create_graph_namespace_cache(void);
static void invalidate_graph_caches(Datum arg, int cache_id,
                                    uint32 hash_value);
static void flush_graph_name_cache(void);
static void flush_graph_namespace_cache(void);
static graph_cache_data *search_graph_name_cache_miss(Name name);
static graph_cache_data *search_graph_namespace_cache_miss(Oid namespace);
static void fill_graph_cache_data(graph_cache_data *cache_data,
                                  HeapTuple tuple, TupleDesc tuple_desc);

/* ag_label */
static void initialize_label_caches(void);
static void create_label_caches(void);
static void create_label_name_graph_cache(void);
static void create_label_graph_oid_cache(void);
static void create_label_relation_cache(void);
static void create_label_seq_name_graph_cache(void);
static void invalidate_label_caches(Datum arg, Oid relid);
static void invalidate_label_name_graph_cache(Oid relid);
static void flush_label_name_graph_cache(void);
static void invalidate_label_graph_oid_cache(Oid relid);
static void flush_label_graph_oid_cache(void);
static void invalidate_label_relation_cache(Oid relid);
static void flush_label_relation_cache(void);
static void invalidate_label_seq_name_graph_cache(Oid relid);
static void flush_label_seq_name_graph_cache(void);

static label_cache_data *search_label_name_graph_cache_miss(Name name,
                                                            Oid graph);
static void *label_name_graph_cache_hash_search(Name name, Oid graph,
                                                HASHACTION action,
                                                bool *found);
static label_cache_data *search_label_graph_oid_cache_miss(Oid graph,
                                                           uint32 id);
static void *label_graph_oid_cache_hash_search(uint32 graph, int32 id,
                                               HASHACTION action, bool *found);
static label_cache_data *search_label_relation_cache_miss(Oid relation);
static label_cache_data *search_label_seq_name_graph_cache_miss(Name name,
                                                                Oid graph);
static void *label_seq_name_graph_cache_hash_search(Name name, Oid graph,
                                                    HASHACTION action,
                                                    bool *found);

static void fill_label_cache_data(label_cache_data *cache_data,
                                  HeapTuple tuple, TupleDesc tuple_desc);

/* ag_graph_schema caches */
static void initialize_edge_schema_caches(void);
static void create_edge_schema_cache(void);
static void create_start_vertex_end_labels_cache(void);
static void create_end_vertex_start_labels_cache(void);
static void flush_edge_schema_cache(void);
static void flush_start_vertex_end_labels_cache(void);
static void flush_end_vertex_start_labels_cache(void);
static edge_schema_cache_data *search_edge_schema_cache_miss(Oid graph, int32 edge_label_id);
static vertex_edge_labels_cache_data *search_start_vertex_end_labels_cache_miss(Oid graph, int32 start_label_id);
static vertex_edge_labels_cache_data *search_end_vertex_start_labels_cache_miss(Oid graph, int32 end_label_id);

static void initialize_caches(void)
{
    static bool initialized = false;

    if (initialized)
    {
        return;
    }
    if (!CacheMemoryContext)
    {
        CreateCacheMemoryContext();
    }
    initialize_graph_caches();
    initialize_label_caches();
    initialize_edge_schema_caches();

    initialized = true;
}

static void ag_cache_scan_key_init(ScanKey entry, AttrNumber attno,
                                   RegProcedure func)
{
    entry->sk_flags = 0;
    entry->sk_attno = attno;
    entry->sk_strategy = BTEqualStrategyNumber;
    entry->sk_subtype = InvalidOid;
    entry->sk_collation = C_COLLATION_OID;
    fmgr_info_cxt(func, &entry->sk_func, CacheMemoryContext);
    entry->sk_argument = (Datum)0;
}

static int name_hash_compare(const void *key1, const void *key2, Size keysize)
{
    Name name1 = (Name)key1;
    Name name2 = (Name)key2;

    /* keysize parameter is superfluous here */
    Assert(keysize == NAMEDATALEN);

    return strncmp(NameStr(*name1), NameStr(*name2), NAMEDATALEN);
}

static void initialize_graph_caches(void)
{
    /* ag_graph.name */
    ag_cache_scan_key_init(&graph_name_scan_keys[0], Anum_ag_graph_name,
                           F_NAMEEQ);

    /* ag_graph.namespace */
    ag_cache_scan_key_init(&graph_namespace_scan_keys[0],
                           Anum_ag_graph_namespace, F_OIDEQ);

    create_graph_caches();

    /*
     * A graph is backed by the bound namespace. So, register the invalidation
     * logic of the graph caches for invalidation events of NAMESPACEOID cache.
     */
    CacheRegisterSyscacheCallback(NAMESPACEOID, invalidate_graph_caches,
                                  (Datum)0);
}

static void create_graph_caches(void)
{
    /*
     * All the hash tables are created using their dedicated memory contexts
     * which are under TopMemoryContext.
     */
    create_graph_name_cache();
    create_graph_namespace_cache();
}

static void create_graph_name_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(NameData);
    hash_ctl.entrysize = sizeof(graph_name_cache_entry);
    hash_ctl.match = name_hash_compare;

    /*
     * Please see the comment of hash_create() for the nelem value 16 here.
     * HASH_BLOBS flag is set because the key for this hash is fixed-size.
     */
    graph_name_cache_hash = hash_create("ag_graph (name) cache", 16, &hash_ctl,
                                        HASH_ELEM | HASH_BLOBS | HASH_COMPARE);
}

static void create_graph_namespace_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(graph_namespace_cache_entry);

    /*
     * Please see the comment of hash_create() for the nelem value 16 here.
     * HASH_BLOBS flag is set because the size of the key is sizeof(uint32).
     */
    graph_namespace_cache_hash = hash_create("ag_graph (namespace) cache", 16,
                                             &hash_ctl,
                                             HASH_ELEM | HASH_BLOBS);
}

static void invalidate_graph_caches(Datum arg, int cache_id, uint32 hash_value)
{
    Assert(graph_name_cache_hash);

    /*
     * Currently, all entries in the graph caches are flushed because
     * hash_value is for an entry in NAMESPACEOID cache. Since the caches
     * are not currently used in performance-critical paths, this seems OK.
     */
    flush_graph_name_cache();
    flush_graph_namespace_cache();
}

static void flush_graph_name_cache(void)
{
    /*
     * If the graph_name_cache exists, destroy it. This will avoid any
     * potential corruption issues.
     */
    if (graph_name_cache_hash)
    {
        hash_destroy(graph_name_cache_hash);
        graph_name_cache_hash = NULL;
    }

    /* recreate the graph_name_cache */
    create_graph_name_cache();
}

static void flush_graph_namespace_cache(void)
{
    /*
     * If the graph_namespace_cache exists, destroy it. This will avoid any
     * potential corruption issues.
     */
    if (graph_namespace_cache_hash)
    {
        hash_destroy(graph_namespace_cache_hash);
        graph_namespace_cache_hash = NULL;
    }

    /* recreate the graph_namespace_cache */
    create_graph_namespace_cache();
}

graph_cache_data *search_graph_name_cache(const char *name)
{
    NameData name_key;
    graph_name_cache_entry *entry;

    Assert(name);

    initialize_caches();

    namestrcpy(&name_key, name);
    entry = hash_search(graph_name_cache_hash, &name_key, HASH_FIND, NULL);
    if (entry)
    {
        return &entry->data;
    }

    return search_graph_name_cache_miss(&name_key);
}

static graph_cache_data *search_graph_name_cache_miss(Name name)
{
    ScanKeyData scan_keys[1];
    Relation ag_graph;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    graph_name_cache_entry *entry;

    memcpy(scan_keys, graph_name_scan_keys, sizeof(graph_name_scan_keys));
    scan_keys[0].sk_argument = NameGetDatum(name);

    /*
     * Calling table_open() might call AcceptInvalidationMessage() and that
     * might flush the graph caches. This is OK because this function is called
     * when the desired entry is not in the cache.
     */
    ag_graph = table_open(ag_graph_relation_id(), AccessShareLock);
    scan_desc = systable_beginscan(ag_graph, ag_graph_name_index_id(), true,
                                   NULL, 1, scan_keys);

    /* don't need to loop over scan_desc because ag_graph_name_index is UNIQUE */
    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan_desc);
        table_close(ag_graph, AccessShareLock);

        return NULL;
    }

    /* get a new entry */
    entry = hash_search(graph_name_cache_hash, name, HASH_ENTER, &found);
    Assert(!found); /* no concurrent update on graph_name_cache_hash */

    /* fill the new entry with the retrieved tuple */
    fill_graph_cache_data(&entry->data, tuple, RelationGetDescr(ag_graph));

    systable_endscan(scan_desc);
    table_close(ag_graph, AccessShareLock);

    return &entry->data;
}

graph_cache_data *search_graph_namespace_cache(Oid namespace)
{
    graph_namespace_cache_entry *entry;

    initialize_caches();

    entry = hash_search(graph_namespace_cache_hash, &namespace, HASH_FIND,
                        NULL);
    if (entry)
    {
        return &entry->data;
    }

    return search_graph_namespace_cache_miss(namespace);
}

static graph_cache_data *search_graph_namespace_cache_miss(Oid namespace)
{
    ScanKeyData scan_keys[1];
    Relation ag_graph;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    graph_namespace_cache_entry *entry;

    memcpy(scan_keys, graph_namespace_scan_keys,
           sizeof(graph_namespace_scan_keys));
    scan_keys[0].sk_argument = ObjectIdGetDatum(namespace);

    /*
     * Calling table_open() might call AcceptInvalidationMessage() and that
     * might flush the graph caches. This is OK because this function is called
     * when the desired entry is not in the cache.
     */
    ag_graph = table_open(ag_graph_relation_id(), AccessShareLock);
    scan_desc = systable_beginscan(ag_graph, ag_graph_namespace_index_id(),
                                   true, NULL, 1, scan_keys);

    /* don't need to loop over scan_desc because ag_graph_namespace_index is */
    /* UNIQUE */
    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan_desc);
        table_close(ag_graph, AccessShareLock);

        return NULL;
    }

    /* get a new entry */
    entry = hash_search(graph_namespace_cache_hash, &namespace, HASH_ENTER,
                        &found);
    Assert(!found); /* no concurrent update on graph_namespace_cache_hash */

    /* fill the new entry with the retrieved tuple */
    fill_graph_cache_data(&entry->data, tuple, RelationGetDescr(ag_graph));

    systable_endscan(scan_desc);
    table_close(ag_graph, AccessShareLock);

    return &entry->data;
}

static void fill_graph_cache_data(graph_cache_data *cache_data,
                                  HeapTuple tuple, TupleDesc tuple_desc)
{
    bool is_null;
    Datum value;
    Name name;

    /* ag_graph.id */
    value = heap_getattr(tuple, Anum_ag_graph_oid, tuple_desc, &is_null);
    Assert(!is_null);
    cache_data->oid = DatumGetObjectId(value);
    /* ag_graph.name */
    value = heap_getattr(tuple, Anum_ag_graph_name, tuple_desc, &is_null);
    Assert(!is_null);
    name = DatumGetName(value);
    Assert(name != NULL);
    namestrcpy(&cache_data->name, name->data);
    /* ag_graph.namespace */
    value = heap_getattr(tuple, Anum_ag_graph_namespace, tuple_desc, &is_null);
    Assert(!is_null);
    cache_data->namespace = DatumGetObjectId(value);
}

static void initialize_label_caches(void)
{
    /* ag_label.name, ag_label.graph */
    ag_cache_scan_key_init(&label_name_graph_scan_keys[0], Anum_ag_label_name,
                           F_NAMEEQ);
    ag_cache_scan_key_init(&label_name_graph_scan_keys[1], Anum_ag_label_graph,
                           F_INT4EQ);

    /* ag_label.graph, ag_label.id */
    ag_cache_scan_key_init(&label_graph_oid_scan_keys[0], Anum_ag_label_graph,
                           F_INT4EQ);
    ag_cache_scan_key_init(&label_graph_oid_scan_keys[1], Anum_ag_label_id,
                           F_INT4EQ);

    /* ag_label.relation */
    ag_cache_scan_key_init(&label_relation_scan_keys[0],
                           Anum_ag_label_relation, F_OIDEQ);
    
    /* ag_label.seq_name, ag_label.graph */
    ag_cache_scan_key_init(&label_seq_name_graph_scan_keys[0], Anum_ag_label_seq_name,
                           F_NAMEEQ);
    ag_cache_scan_key_init(&label_seq_name_graph_scan_keys[1], Anum_ag_label_graph,
                           F_OIDEQ);

    /* ag_label.seq_name, ag_label.graph */
    ag_cache_scan_key_init(&label_seq_name_graph_scan_keys[0],
                           Anum_ag_label_seq_name, F_NAMEEQ);
    ag_cache_scan_key_init(&label_seq_name_graph_scan_keys[1],
                           Anum_ag_label_graph, F_OIDEQ);

    /* ag_label.seq_name, ag_label.graph */
    ag_cache_scan_key_init(&label_seq_name_graph_scan_keys[0],
                           Anum_ag_label_seq_name, F_NAMEEQ);
    ag_cache_scan_key_init(&label_seq_name_graph_scan_keys[1],
                           Anum_ag_label_graph, F_OIDEQ);

    create_label_caches();

    /*
     * A label is backed by the bound relation. So, register the invalidation
     * logic of the label caches for invalidation events of relation cache.
     */
    CacheRegisterRelcacheCallback(invalidate_label_caches, (Datum)0);
}

static void create_label_caches(void)
{
    /*
     * All the hash tables are created using their dedicated memory contexts
     * which are under TopMemoryContext.
     */
    create_label_name_graph_cache();
    create_label_graph_oid_cache();
    create_label_relation_cache();
    create_label_seq_name_graph_cache();
}

static void create_label_name_graph_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(label_name_graph_cache_key);
    hash_ctl.entrysize = sizeof(label_name_graph_cache_entry);

    /*
     * Please see the comment of hash_create() for the nelem value 16 here.
     * HASH_BLOBS flag is set because the key for this hash is fixed-size.
     */
    label_name_graph_cache_hash = hash_create("ag_label (name, graph) cache",
                                              16, &hash_ctl,
                                              HASH_ELEM | HASH_BLOBS);
}

static void create_label_graph_oid_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(label_graph_oid_cache_key);
    hash_ctl.entrysize = sizeof(label_graph_oid_cache_entry);

    /*
     * Please see the comment of hash_create() for the nelem value 16 here.
     * HASH_BLOBS flag is set because the key for this hash is fixed-size.
     */
    label_graph_oid_cache_hash = hash_create("ag_label (graph, id) cache", 16,
                                             &hash_ctl,
                                             HASH_ELEM | HASH_BLOBS);
}

static void create_label_relation_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(label_relation_cache_entry);

    /*
     * Please see the comment of hash_create() for the nelem value 16 here.
     * HASH_BLOBS flag is set because the size of the key is sizeof(uint32).
     */
    label_relation_cache_hash = hash_create("ag_label (relation) cache", 16,
                                            &hash_ctl, HASH_ELEM | HASH_BLOBS);
}

static void create_label_seq_name_graph_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(label_seq_name_graph_cache_key);
    hash_ctl.entrysize = sizeof(label_seq_name_graph_cache_entry);

    /*
     * Please see the comment of hash_create() for the nelem value 16 here.
     * HASH_BLOBS flag is set because the key for this hash is fixed-size.
     */
    label_seq_name_graph_cache_hash = hash_create("ag_label (seq_name, graph) cache",
                                                  16, &hash_ctl,
                                                  HASH_ELEM | HASH_BLOBS);
}

static void invalidate_label_caches(Datum arg, Oid relid)
{
    Assert(label_name_graph_cache_hash);
    Assert(label_seq_name_graph_cache_hash);


    if (OidIsValid(relid))
    {
        invalidate_label_name_graph_cache(relid);
        invalidate_label_graph_oid_cache(relid);
        invalidate_label_relation_cache(relid);
        invalidate_label_seq_name_graph_cache(relid);
    }
    else
    {
        flush_label_name_graph_cache();
        flush_label_graph_oid_cache();
        flush_label_relation_cache();
        flush_label_seq_name_graph_cache();
    }
}

static void invalidate_label_name_graph_cache(Oid relid)
{
    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, label_name_graph_cache_hash);
    for (;;)
    {
        label_name_graph_cache_entry *entry;
        void *removed;

        entry = hash_seq_search(&hash_seq);
        if (!entry)
        {
            break;
        }
        if (entry->data.relation != relid)
        {
            continue;
        }
        removed = hash_search(label_name_graph_cache_hash, &entry->key,
                              HASH_REMOVE, NULL);
        hash_seq_term(&hash_seq);

        if (!removed)
        {
            ereport(ERROR,
                    (errmsg_internal("label (name, graph) cache corrupted")));
        }

        break;
    }
}

static void flush_label_name_graph_cache(void)
{
    /*
     * If the label_name_graph_cache exists, destroy it. This will avoid any
     * potential corruption issues.
     */
    if (label_name_graph_cache_hash)
    {
        hash_destroy(label_name_graph_cache_hash);
        label_name_graph_cache_hash = NULL;
    }

    /* recreate the label_name_graph_cache */
    create_label_name_graph_cache();
}

static void invalidate_label_graph_oid_cache(Oid relid)
{
    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, label_graph_oid_cache_hash);
    for (;;)
    {
        label_graph_oid_cache_entry *entry;
        void *removed;

        entry = hash_seq_search(&hash_seq);
        if (!entry)
        {
            break;
        }
        if (entry->data.relation != relid)
        {
            continue;
        }
        removed = hash_search(label_graph_oid_cache_hash, &entry->key,
                              HASH_REMOVE, NULL);
        hash_seq_term(&hash_seq);

        if (!removed)
        {
            ereport(ERROR,
                    (errmsg_internal("label (graph, id) cache corrupted")));
        }

        break;
    }
}

static void flush_label_graph_oid_cache(void)
{
    /*
     * If the label_graph_oid_cache exists, destroy it. This will avoid any
     * potential corruption issues.
     */
    if (label_graph_oid_cache_hash)
    {
        hash_destroy(label_graph_oid_cache_hash);
        label_graph_oid_cache_hash = NULL;
    }

    /* recreate the label_graph_oid_cache */
    create_label_graph_oid_cache();
}

static void invalidate_label_relation_cache(Oid relid)
{
    label_relation_cache_entry *entry;
    void *removed;

    entry = hash_search(label_relation_cache_hash, &relid, HASH_FIND, NULL);
    if (!entry)
    {
        return;
    }
    removed = hash_search(label_relation_cache_hash, &relid, HASH_REMOVE,
                          NULL);
    if (!removed)
    {
        ereport(ERROR, (errmsg_internal("label (namespace) cache corrupted")));
    }
}

static void flush_label_relation_cache(void)
{
    /*
     * If the label_relation_cache exists, destroy it. This will avoid any
     * potential corruption issues.
     */
    if (label_relation_cache_hash)
    {
        hash_destroy(label_relation_cache_hash);
        label_relation_cache_hash = NULL;
    }

    /* recreate the label_relation_cache */
    create_label_relation_cache();
}

static void invalidate_label_seq_name_graph_cache(Oid relid)
{
    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, label_seq_name_graph_cache_hash);
    for (;;)
    {
        label_seq_name_graph_cache_entry *entry;
        void *removed;

        entry = hash_seq_search(&hash_seq);
        if (!entry)
        {
            break;
        }
        if (entry->data.relation != relid)
        {
            continue;
        }
        removed = hash_search(label_seq_name_graph_cache_hash, &entry->key,
                              HASH_REMOVE, NULL);
        hash_seq_term(&hash_seq);

        if (!removed)
        {
            ereport(ERROR,
                    (errmsg_internal("label (seq_name, graph) cache corrupted")));
        }

        break;
    }
}

static void flush_label_seq_name_graph_cache(void)
{
    /*
     * If the label_seq_name_graph_cache exists, destroy it. This will
     * avoid any potential corruption issues by deleting entries.
     */
    if (label_seq_name_graph_cache_hash)
    {
        hash_destroy(label_seq_name_graph_cache_hash);
        label_seq_name_graph_cache_hash = NULL;
    }

    /* recreate the label_seq_name_graph_cache */
    create_label_seq_name_graph_cache();
}

label_cache_data *search_label_name_graph_cache(const char *name, Oid graph)
{
    NameData name_key;
    label_name_graph_cache_entry *entry;

    Assert(name);

    initialize_caches();

    namestrcpy(&name_key, name);
    entry = label_name_graph_cache_hash_search(&name_key, graph, HASH_FIND,
                                               NULL);
    if (entry)
    {
        return &entry->data;
    }
    return search_label_name_graph_cache_miss(&name_key, graph);
}

static label_cache_data *search_label_name_graph_cache_miss(Name name,
                                                            Oid graph)
{
    ScanKeyData scan_keys[2];
    Relation ag_label;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    label_name_graph_cache_entry *entry;

    memcpy(scan_keys, label_name_graph_scan_keys,
           sizeof(label_name_graph_scan_keys));
    scan_keys[0].sk_argument = NameGetDatum(name);
    scan_keys[1].sk_argument = ObjectIdGetDatum(graph);

    /*
     * Calling table_open() might call AcceptInvalidationMessage() and that
     * might invalidate the label caches. This is OK because this function is
     * called when the desired entry is not in the cache.
     */
    ag_label = table_open(ag_label_relation_id(), AccessShareLock);
    scan_desc = systable_beginscan(ag_label, ag_label_name_graph_index_id(),
                                   true, NULL, 2, scan_keys);

    /*
     * don't need to loop over scan_desc because ag_label_name_graph_index is
     * UNIQUE
     */
    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan_desc);
        table_close(ag_label, AccessShareLock);

        return NULL;
    }

    /* get a new entry */
    entry = label_name_graph_cache_hash_search(name, graph, HASH_ENTER,
                                               &found);
    Assert(!found); /* no concurrent update on label_name_graph_cache_hash */

    /* fill the new entry with the retrieved tuple */
    fill_label_cache_data(&entry->data, tuple, RelationGetDescr(ag_label));

    systable_endscan(scan_desc);
    table_close(ag_label, AccessShareLock);

    return &entry->data;
}

static void *label_name_graph_cache_hash_search(Name name, Oid graph,
                                                HASHACTION action, bool *found)
{
    label_name_graph_cache_key key;

    /* initialize the hash key for label_name_graph_cache_hash */
    namestrcpy(&key.name, name->data);
    key.graph = graph;

    return hash_search(label_name_graph_cache_hash, &key, action, found);
}

label_cache_data *search_label_graph_oid_cache(uint32 graph_oid, int32 id)
{
    label_graph_oid_cache_entry *entry;

    Assert(label_id_is_valid(id));

    initialize_caches();

    entry = label_graph_oid_cache_hash_search(graph_oid, id, HASH_FIND, NULL);
    if (entry)
    {
        return &entry->data;
    }
    return search_label_graph_oid_cache_miss(graph_oid, id);
}

static label_cache_data *search_label_graph_oid_cache_miss(Oid graph, uint32 id)
{
    ScanKeyData scan_keys[2];
    Relation ag_label;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    label_graph_oid_cache_entry *entry;

    memcpy(scan_keys, label_graph_oid_scan_keys,
           sizeof(label_graph_oid_scan_keys));
    scan_keys[0].sk_argument = ObjectIdGetDatum(graph);
    scan_keys[1].sk_argument = Int32GetDatum(id);

    /*
     * Calling table_open() might call AcceptInvalidationMessage() and that
     * might invalidate the label caches. This is OK because this function is
     * called when the desired entry is not in the cache.
     */
    ag_label = table_open(ag_label_relation_id(), AccessShareLock);
    scan_desc = systable_beginscan(ag_label, ag_label_graph_oid_index_id(), true,
                                   NULL, 2, scan_keys);

    /*
     * don't need to loop over scan_desc because ag_label_graph_oid_index is
     * UNIQUE
     */
    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan_desc);
        table_close(ag_label, AccessShareLock);

        return NULL;
    }

    /* get a new entry */
    entry = label_graph_oid_cache_hash_search(graph, id, HASH_ENTER, &found);
    Assert(!found); /* no concurrent update on label_graph_oid_cache_hash */

    /* fill the new entry with the retrieved tuple */
    fill_label_cache_data(&entry->data, tuple, RelationGetDescr(ag_label));

    systable_endscan(scan_desc);
    table_close(ag_label, AccessShareLock);

    return &entry->data;
}

static void *label_graph_oid_cache_hash_search(uint32 graph, int32 id,
                                               HASHACTION action, bool *found)
{
    label_graph_oid_cache_key key;

    /* initialize the hash key for label_graph_oid_cache_hash */
    key.graph = graph;
    key.id = id;

    return hash_search(label_graph_oid_cache_hash, &key, action, found);
}

label_cache_data *search_label_relation_cache(Oid relation)
{
    label_relation_cache_entry *entry;

    initialize_caches();

    entry = hash_search(label_relation_cache_hash, &relation, HASH_FIND, NULL);
    if (entry)
    {
        return &entry->data;
    }
    return search_label_relation_cache_miss(relation);
}

static label_cache_data *search_label_relation_cache_miss(Oid relation)
{
    ScanKeyData scan_keys[1];
    Relation ag_label;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    label_cache_data *entry;

    memcpy(scan_keys, label_relation_scan_keys,
           sizeof(label_relation_scan_keys));
    scan_keys[0].sk_argument = ObjectIdGetDatum(relation);

    /*
     * Calling table_open() might call AcceptInvalidationMessage() and that
     * might invalidate the label caches. This is OK because this function is
     * called when the desired entry is not in the cache.
     */
    ag_label = table_open(ag_label_relation_id(), AccessShareLock);
    scan_desc = systable_beginscan(ag_label, ag_label_relation_index_id(), true,
                                   NULL, 1, scan_keys);

    /* don't need to loop over scan_desc because ag_label_relation_index is */
    /* UNIQUE */
    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan_desc);
        table_close(ag_label, AccessShareLock);

        return NULL;
    }

    /* get a new entry */
    entry = hash_search(label_relation_cache_hash, &relation, HASH_ENTER,
                        &found);
    Assert(!found); /* no concurrent update on label_relation_cache_hash */

    /* fill the new entry with the retrieved tuple */
    fill_label_cache_data(entry, tuple, RelationGetDescr(ag_label));

    systable_endscan(scan_desc);
    table_close(ag_label, AccessShareLock);

    return entry;
}

label_cache_data *search_label_seq_name_graph_cache(const char *name, Oid graph)
{
    NameData name_key;
    label_seq_name_graph_cache_entry *entry;

    Assert(name);
    Assert(OidIsValid(graph));

    initialize_caches();

    namestrcpy(&name_key, name);
    entry = label_seq_name_graph_cache_hash_search(&name_key, graph, HASH_FIND,
                                                   NULL);
    if (entry)
    {
        return &entry->data;
    }
    return search_label_seq_name_graph_cache_miss(&name_key, graph);
}

static label_cache_data *search_label_seq_name_graph_cache_miss(Name name,
                                                                Oid graph)
{
    ScanKeyData scan_keys[2];
    Relation ag_label;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    label_seq_name_graph_cache_entry *entry;

    memcpy(scan_keys, label_seq_name_graph_scan_keys,
           sizeof(label_seq_name_graph_scan_keys));
    scan_keys[0].sk_argument = NameGetDatum(name);
    scan_keys[1].sk_argument = ObjectIdGetDatum(graph);

    /*
     * Calling table_open() might call AcceptInvalidationMessage() and that
     * might invalidate the label caches. This is OK because this function is
     * called when the desired entry is not in the cache.
     */
    ag_label = table_open(ag_label_relation_id(), AccessShareLock);
    scan_desc = systable_beginscan(ag_label, ag_label_seq_name_graph_index_id(),
                                   true, NULL, 2, scan_keys);

    /*
     * don't need to loop over scan_desc because ag_label_seq_name_graph_index is
     * UNIQUE
     */
    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(scan_desc);
        table_close(ag_label, AccessShareLock);

        return NULL;
    }

    /* get a new entry */
    entry = label_seq_name_graph_cache_hash_search(name, graph, HASH_ENTER,
                                                   &found);
    Assert(!found); /* no concurrent update on label_seq_name_graph_cache_hash */

    /* fill the new entry with the retrieved tuple */
    fill_label_cache_data(&entry->data, tuple, RelationGetDescr(ag_label));

    systable_endscan(scan_desc);
    table_close(ag_label, AccessShareLock);

    return &entry->data;
}

static void *label_seq_name_graph_cache_hash_search(Name name, Oid graph,
                                                    HASHACTION action,
                                                    bool *found)
{
    label_seq_name_graph_cache_key key;

    /* initialize the hash key for label_seq_name_graph_cache_hash */
    namestrcpy(&key.name, name->data);
    key.graph = graph;

    return hash_search(label_seq_name_graph_cache_hash, &key, action, found);
}

static void fill_label_cache_data(label_cache_data *cache_data,
                                  HeapTuple tuple, TupleDesc tuple_desc)
{
    bool is_null;
    Datum value;
    Name name;

    /* ag_label.name */
    value = heap_getattr(tuple, Anum_ag_label_name, tuple_desc, &is_null);
    Assert(!is_null);
    name = DatumGetName(value);
    Assert(name != NULL);
    namestrcpy(&cache_data->name, name->data);
    /* ag_label.graph */
    value = heap_getattr(tuple, Anum_ag_label_graph, tuple_desc, &is_null);
    Assert(!is_null);
    cache_data->graph = DatumGetObjectId(value);
    /* ag_label.id */
    value = heap_getattr(tuple, Anum_ag_label_id, tuple_desc, &is_null);
    Assert(!is_null);
    cache_data->id = DatumGetInt32(value);
    /* ag_label.kind */
    value = heap_getattr(tuple, Anum_ag_label_kind, tuple_desc, &is_null);
    Assert(!is_null);
    cache_data->kind = DatumGetChar(value);
    /* ag_label.relation */
    value = heap_getattr(tuple, Anum_ag_label_relation, tuple_desc, &is_null);
    Assert(!is_null);
    cache_data->relation = DatumGetObjectId(value);
    /* ag_label.seq_name */
    value = heap_getattr(tuple, Anum_ag_label_seq_name, tuple_desc, &is_null);
    Assert(!is_null);
    namestrcpy(&cache_data->seq_name, DatumGetName(value)->data);
}

/*
 * Edge Schema Cache Implementation
 *
 * Three separate caches for efficient lookups:
 * 1. edge_schema_cache: (graph, edge_label_id) -> (start_label_id, end_label_id) pairs
 * 2. start_vertex_end_labels_cache: (graph, start_label_id) -> list of end_label_ids
 * 3. end_vertex_start_labels_cache: (graph, end_label_id) -> list of start_label_ids
 */

static void initialize_edge_schema_caches(void)
{
    /* ag_graph_schema.graph, ag_graph_schema.edge_label_id */
    ag_cache_scan_key_init(&edge_schema_scan_keys[0],
                           Anum_ag_graph_schema_graph, F_OIDEQ);
    ag_cache_scan_key_init(&edge_schema_scan_keys[1],
                           Anum_ag_graph_schema_edge_label_id, F_INT4EQ);

    create_edge_schema_cache();
    create_start_vertex_end_labels_cache();
    create_end_vertex_start_labels_cache();
}

static void create_edge_schema_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(edge_schema_cache_key);
    hash_ctl.entrysize = sizeof(edge_schema_cache_entry);

    edge_schema_cache_hash = hash_create("ag_graph_schema (graph, edge_label_id) cache",
                                         16, &hash_ctl,
                                         HASH_ELEM | HASH_BLOBS);
}

static void create_start_vertex_end_labels_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(vertex_labels_cache_key);
    hash_ctl.entrysize = sizeof(vertex_labels_cache_entry);

    start_vertex_end_labels_cache_hash = hash_create("ag_graph_schema start->end labels cache",
                                                     16, &hash_ctl,
                                                     HASH_ELEM | HASH_BLOBS);
}

static void create_end_vertex_start_labels_cache(void)
{
    HASHCTL hash_ctl;

    MemSet(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(vertex_labels_cache_key);
    hash_ctl.entrysize = sizeof(vertex_labels_cache_entry);

    end_vertex_start_labels_cache_hash = hash_create("ag_graph_schema end->start labels cache",
                                                     16, &hash_ctl,
                                                     HASH_ELEM | HASH_BLOBS);
}

static void flush_edge_schema_cache(void)
{
    HASH_SEQ_STATUS hash_seq;
    edge_schema_cache_entry *entry;

    if (!edge_schema_cache_hash)
        return;

    /* Free the entries array in each cached entry before destroying hash */
    hash_seq_init(&hash_seq, edge_schema_cache_hash);
    while ((entry = hash_seq_search(&hash_seq)) != NULL)
    {
        if (entry->data.entries != NULL)
        {
            pfree(entry->data.entries);
            entry->data.entries = NULL;
        }
    }

    hash_destroy(edge_schema_cache_hash);
    edge_schema_cache_hash = NULL;
    create_edge_schema_cache();
}

static void flush_start_vertex_end_labels_cache(void)
{
    HASH_SEQ_STATUS hash_seq;
    vertex_labels_cache_entry *entry;

    if (!start_vertex_end_labels_cache_hash)
        return;

    hash_seq_init(&hash_seq, start_vertex_end_labels_cache_hash);
    while ((entry = hash_seq_search(&hash_seq)) != NULL)
    {
        if (entry->data.label_ids != NULL)
        {
            pfree(entry->data.label_ids);
            entry->data.label_ids = NULL;
        }
    }

    hash_destroy(start_vertex_end_labels_cache_hash);
    start_vertex_end_labels_cache_hash = NULL;
    create_start_vertex_end_labels_cache();
}

static void flush_end_vertex_start_labels_cache(void)
{
    HASH_SEQ_STATUS hash_seq;
    vertex_labels_cache_entry *entry;

    if (!end_vertex_start_labels_cache_hash)
        return;

    hash_seq_init(&hash_seq, end_vertex_start_labels_cache_hash);
    while ((entry = hash_seq_search(&hash_seq)) != NULL)
    {
        if (entry->data.label_ids != NULL)
        {
            pfree(entry->data.label_ids);
            entry->data.label_ids = NULL;
        }
    }

    hash_destroy(end_vertex_start_labels_cache_hash);
    end_vertex_start_labels_cache_hash = NULL;
    create_end_vertex_start_labels_cache();
}

/*
 * Invalidate all edge schema caches for a specific graph.
 * Called when edge schema entries are modified (insert/delete).
 */
void invalidate_edge_schema_caches_for_graph(Oid graph)
{
    /* For simplicity, flush all caches when any graph's schema changes */
    /* A more sophisticated implementation could track per-graph entries */
    if (edge_schema_cache_hash)
        flush_edge_schema_cache();
    if (start_vertex_end_labels_cache_hash)
        flush_start_vertex_end_labels_cache();
    if (end_vertex_start_labels_cache_hash)
        flush_end_vertex_start_labels_cache();
}

/*
 * Search edge schema cache by (graph, edge_label_id).
 */
edge_schema_cache_data *search_edge_schema_cache(Oid graph, int32 edge_label_id)
{
    edge_schema_cache_key key;
    edge_schema_cache_entry *entry;
    bool found;

    initialize_caches();

    key.graph = graph;
    key.edge_label_id = edge_label_id;

    entry = hash_search(edge_schema_cache_hash, &key, HASH_FIND, &found);
    if (found && entry)
    {
        return &entry->data;
    }

    return search_edge_schema_cache_miss(graph, edge_label_id);
}

static edge_schema_cache_data *search_edge_schema_cache_miss(Oid graph, int32 edge_label_id)
{
    ScanKeyData scan_keys[2];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    edge_schema_cache_key key;
    edge_schema_cache_entry *entry;
    List *entries_list = NIL;
    ListCell *lc;
    int i;
    TupleDesc tupdesc;

    memcpy(scan_keys, edge_schema_scan_keys, sizeof(edge_schema_scan_keys));
    scan_keys[0].sk_argument = ObjectIdGetDatum(graph);
    scan_keys[1].sk_argument = Int32GetDatum(edge_label_id);

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), AccessShareLock);
    tupdesc = RelationGetDescr(ag_graph_schema);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 2, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        edge_schema_entry *schema_entry;
        bool isnull;

        schema_entry = MemoryContextAlloc(CacheMemoryContext,
                                          sizeof(edge_schema_entry));

        schema_entry->start_label_id = DatumGetInt32(
            heap_getattr(tuple, Anum_ag_graph_schema_start_label_id, tupdesc, &isnull));
        schema_entry->end_label_id = DatumGetInt32(
            heap_getattr(tuple, Anum_ag_graph_schema_end_label_id, tupdesc, &isnull));

        entries_list = lappend(entries_list, schema_entry);
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, AccessShareLock);

    /* Get a new cache entry */
    key.graph = graph;
    key.edge_label_id = edge_label_id;
    entry = hash_search(edge_schema_cache_hash, &key, HASH_ENTER, &found);
    Assert(!found);

    /* Fill the cache entry */
    entry->data.graph = graph;
    entry->data.edge_label_id = edge_label_id;
    entry->data.num_entries = list_length(entries_list);

    if (entry->data.num_entries > 0)
    {
        entry->data.entries = MemoryContextAlloc(CacheMemoryContext,
                                                  sizeof(edge_schema_entry) * entry->data.num_entries);
        i = 0;
        foreach(lc, entries_list)
        {
            edge_schema_entry *src = (edge_schema_entry *) lfirst(lc);
            entry->data.entries[i] = *src;
            pfree(src);
            i++;
        }
    }
    else
    {
        entry->data.entries = NULL;
    }

    list_free(entries_list);

    return &entry->data;
}

/*
 * Search start_vertex -> end_labels cache.
 */
vertex_edge_labels_cache_data *search_start_vertex_end_labels_cache(Oid graph, int32 start_label_id)
{
    vertex_labels_cache_key key;
    vertex_labels_cache_entry *entry;
    bool found;

    initialize_caches();

    key.graph = graph;
    key.vertex_label_id = start_label_id;

    entry = hash_search(start_vertex_end_labels_cache_hash, &key, HASH_FIND, &found);
    if (found && entry)
    {
        return &entry->data;
    }

    return search_start_vertex_end_labels_cache_miss(graph, start_label_id);
}

static vertex_edge_labels_cache_data *search_start_vertex_end_labels_cache_miss(Oid graph, int32 start_label_id)
{
    ScanKeyData scan_keys[2];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    vertex_labels_cache_key key;
    vertex_labels_cache_entry *entry;
    List *label_ids_list = NIL;
    ListCell *lc;
    int i;
    TupleDesc tupdesc;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph));
    ScanKeyInit(&scan_keys[1], Anum_ag_graph_schema_start_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(start_label_id));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), AccessShareLock);
    tupdesc = RelationGetDescr(ag_graph_schema);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 2, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        bool isnull;
        int32 end_label_id;

        end_label_id = DatumGetInt32(
            heap_getattr(tuple, Anum_ag_graph_schema_end_label_id, tupdesc, &isnull));

        /* Dedup */
        if (!list_member_int(label_ids_list, end_label_id))
            label_ids_list = lappend_int(label_ids_list, end_label_id);
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, AccessShareLock);

    /* Get a new cache entry */
    key.graph = graph;
    key.vertex_label_id = start_label_id;
    entry = hash_search(start_vertex_end_labels_cache_hash, &key, HASH_ENTER, &found);
    Assert(!found);

    /* Fill the cache entry */
    entry->data.graph = graph;
    entry->data.vertex_label_id = start_label_id;
    entry->data.num_label_ids = list_length(label_ids_list);

    if (entry->data.num_label_ids > 0)
    {
        entry->data.label_ids = MemoryContextAlloc(CacheMemoryContext,
                                                    sizeof(int32) * entry->data.num_label_ids);
        i = 0;
        foreach(lc, label_ids_list)
        {
            entry->data.label_ids[i] = lfirst_int(lc);
            i++;
        }
    }
    else
    {
        entry->data.label_ids = NULL;
    }

    list_free(label_ids_list);

    return &entry->data;
}

/*
 * Search end_vertex -> start_labels cache.
 */
vertex_edge_labels_cache_data *search_end_vertex_start_labels_cache(Oid graph, int32 end_label_id)
{
    vertex_labels_cache_key key;
    vertex_labels_cache_entry *entry;
    bool found;

    initialize_caches();

    key.graph = graph;
    key.vertex_label_id = end_label_id;

    entry = hash_search(end_vertex_start_labels_cache_hash, &key, HASH_FIND, &found);
    if (found && entry)
    {
        return &entry->data;
    }

    return search_end_vertex_start_labels_cache_miss(graph, end_label_id);
}

static vertex_edge_labels_cache_data *search_end_vertex_start_labels_cache_miss(Oid graph, int32 end_label_id)
{
    ScanKeyData scan_keys[2];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    bool found;
    vertex_labels_cache_key key;
    vertex_labels_cache_entry *entry;
    List *label_ids_list = NIL;
    ListCell *lc;
    int i;
    TupleDesc tupdesc;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph));
    ScanKeyInit(&scan_keys[1], Anum_ag_graph_schema_end_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(end_label_id));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), AccessShareLock);
    tupdesc = RelationGetDescr(ag_graph_schema);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 2, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        bool isnull;
        int32 start_label_id;

        start_label_id = DatumGetInt32(
            heap_getattr(tuple, Anum_ag_graph_schema_start_label_id, tupdesc, &isnull));

        /* Dedup */
        if (!list_member_int(label_ids_list, start_label_id))
            label_ids_list = lappend_int(label_ids_list, start_label_id);
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, AccessShareLock);

    /* Get a new cache entry */
    key.graph = graph;
    key.vertex_label_id = end_label_id;
    entry = hash_search(end_vertex_start_labels_cache_hash, &key, HASH_ENTER, &found);
    Assert(!found);

    /* Fill the cache entry */
    entry->data.graph = graph;
    entry->data.vertex_label_id = end_label_id;
    entry->data.num_label_ids = list_length(label_ids_list);

    if (entry->data.num_label_ids > 0)
    {
        entry->data.label_ids = MemoryContextAlloc(CacheMemoryContext,
                                                    sizeof(int32) * entry->data.num_label_ids);
        i = 0;
        foreach(lc, label_ids_list)
        {
            entry->data.label_ids[i] = lfirst_int(lc);
            i++;
        }
    }
    else
    {
        entry->data.label_ids = NULL;
    }

    list_free(label_ids_list);

    return &entry->data;
}
