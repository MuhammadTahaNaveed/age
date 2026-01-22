/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
#include "catalog/indexing.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "commands/label_commands.h"
#include "executor/cypher_utils.h"
#include "utils/ag_cache.h"

/*
 * INSERT INTO ag_catalog.ag_label
 * VALUES (label_name, label_graph, label_id, label_kind,
 *         label_relation, seq_name)
 */
void insert_label(const char *label_name, Oid graph_oid, int32 label_id,
                  char label_kind, Oid label_relation, const char *seq_name)
{
    NameData label_name_data;
    NameData seq_name_data;
    Datum values[Natts_ag_label];
    bool nulls[Natts_ag_label];
    Relation ag_label;
    HeapTuple tuple;

    /*
     * NOTE: Is it better to make use of label_id and label_kind domain types
     *       than to use assert to check label_id and label_kind are valid?
     */
    Assert(label_name);
    Assert(label_id_is_valid(label_id));
    Assert(label_kind == LABEL_KIND_VERTEX ||
              label_kind == LABEL_KIND_EDGE);
    Assert(OidIsValid(label_relation));
    Assert(seq_name);

    ag_label = table_open(ag_label_relation_id(), RowExclusiveLock);

    namestrcpy(&label_name_data, label_name);
    values[Anum_ag_label_name - 1] = NameGetDatum(&label_name_data);
    nulls[Anum_ag_label_name - 1] = false;

    values[Anum_ag_label_graph - 1] = ObjectIdGetDatum(graph_oid);
    nulls[Anum_ag_label_graph - 1] = false;

    values[Anum_ag_label_id - 1] = Int32GetDatum(label_id);
    nulls[Anum_ag_label_id - 1] = false;

    values[Anum_ag_label_kind - 1] = CharGetDatum(label_kind);
    nulls[Anum_ag_label_kind - 1] = false;

    values[Anum_ag_label_relation - 1] = ObjectIdGetDatum(label_relation);
    nulls[Anum_ag_label_relation - 1] = false;

    namestrcpy(&seq_name_data, seq_name);
    values[Anum_ag_label_seq_name - 1] = NameGetDatum(&seq_name_data);
    nulls[Anum_ag_label_seq_name - 1] = false;

    tuple = heap_form_tuple(RelationGetDescr(ag_label), values, nulls);

    /*
     * CatalogTupleInsert() is originally for PostgreSQL's catalog. However,
     * it is used at here for convenience.
     */
    CatalogTupleInsert(ag_label, tuple);

    table_close(ag_label, RowExclusiveLock);
}

/* DELETE FROM ag_catalog.ag_label WHERE relation = relation */
void delete_label(Oid relation)
{
    ScanKeyData scan_keys[1];
    Relation ag_label;
    SysScanDesc scan_desc;
    HeapTuple tuple;
    TupleDesc tupdesc;
    Oid graph_oid;
    int32 label_id;
    char label_kind;
    Datum datum;
    bool isnull;

    ScanKeyInit(&scan_keys[0], Anum_ag_label_relation, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(relation));

    ag_label = table_open(ag_label_relation_id(), RowExclusiveLock);
    tupdesc = RelationGetDescr(ag_label);
    scan_desc = systable_beginscan(ag_label, ag_label_relation_index_id(),
                                   true, NULL, 1, scan_keys);

    tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(tuple))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("label (relation=%u) does not exist", relation)));
    }

    /* Extract graph_oid, label_id, and kind before deleting */
    datum = heap_getattr(tuple, Anum_ag_label_graph, tupdesc, &isnull);
    graph_oid = DatumGetObjectId(datum);

    datum = heap_getattr(tuple, Anum_ag_label_id, tupdesc, &isnull);
    label_id = DatumGetInt32(datum);

    datum = heap_getattr(tuple, Anum_ag_label_kind, tupdesc, &isnull);
    label_kind = DatumGetChar(datum);

    CatalogTupleDelete(ag_label, &tuple->t_self);

    systable_endscan(scan_desc);
    table_close(ag_label, RowExclusiveLock);

    /*
     * Clean up edge schema entries that reference this label.
     * For edge labels, delete all entries for this edge.
     * For vertex labels, delete entries where this vertex is start or end.
     */
    if (label_kind == LABEL_KIND_EDGE)
    {
        delete_edge_schema_entries(graph_oid, label_id);
    }
    else if (label_kind == LABEL_KIND_VERTEX)
    {
        delete_edge_schema_entries_for_vertex(graph_oid, label_id);
    }
}

int32 get_label_id(const char *label_name, Oid graph_oid)
{
    label_cache_data *cache_data;

    cache_data = search_label_name_graph_cache(label_name, graph_oid);
    if (cache_data)
        return cache_data->id;
    else
        return INVALID_LABEL_ID;
}

Oid get_label_relation(const char *label_name, Oid graph_oid)
{
    label_cache_data *cache_data;

    cache_data = search_label_name_graph_cache(label_name, graph_oid);
    if (cache_data)
        return cache_data->relation;
    else
        return InvalidOid;
}

char *get_label_relation_name(const char *label_name, Oid graph_oid)
{
    return get_rel_name(get_label_relation(label_name, graph_oid));
}

char get_label_kind(const char *label_name, Oid label_graph)
{
    label_cache_data *cache_data;

    cache_data = search_label_name_graph_cache(label_name, label_graph);
    if (cache_data)
    {
        return cache_data->kind;
    }
    else
    {
        return INVALID_LABEL_ID;
    }
}

char *get_label_seq_relation_name(const char *label_name)
{
    return psprintf("%s_id_seq", label_name);
}

PG_FUNCTION_INFO_V1(_label_name);

/*
 * Using the graph name and the vertex/edge's graphid, find
 * the correct label name from ag_catalog.label
 */
Datum _label_name(PG_FUNCTION_ARGS)
{
    char *label_name;
    label_cache_data *label_cache;
    Oid graph;
    uint32 label_id;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
    {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("graph_oid and label_id must not be null")));
    }

    graph = PG_GETARG_OID(0);
    
    /* Check if the graph OID is valid */
    if (!graph_namespace_exists(graph))
    {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("graph with oid %u does not exist", graph)));
    }

    label_id = (int32)(((uint64)AG_GETARG_GRAPHID(1)) >> ENTRY_ID_BITS);

    label_cache = search_label_graph_oid_cache(graph, label_id);

    label_name = NameStr(label_cache->name);

    /* If label_name is not found, error out */
    if (label_name == NULL)
    {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("label with id %d does not exist in graph %u",
                               label_id, graph)));
    }

    if (IS_AG_DEFAULT_LABEL(label_name))
        PG_RETURN_CSTRING("");

    PG_RETURN_CSTRING(label_name);
}

PG_FUNCTION_INFO_V1(_label_id);

Datum _label_id(PG_FUNCTION_ARGS)
{
    Name graph_name;
    Name label_name;
    Oid graph;
    int32 id;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
    {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("graph_name and label_name must not be null")));
    }
    graph_name = PG_GETARG_NAME(0);
    label_name = PG_GETARG_NAME(1);

    graph = get_graph_oid(NameStr(*graph_name));
    id = get_label_id(NameStr(*label_name), graph);

    PG_RETURN_INT32(id);
}

PG_FUNCTION_INFO_V1(_extract_label_id);

Datum _extract_label_id(PG_FUNCTION_ARGS)
{
    graphid graph_oid;

    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("graph_oid must not be null")));
    }
    graph_oid = AG_GETARG_GRAPHID(0);

    PG_RETURN_INT32(get_graphid_label_id(graph_oid));
}

bool label_id_exists(Oid graph_oid, int32 label_id)
{
    label_cache_data *cache_data;

    cache_data = search_label_graph_oid_cache(graph_oid, label_id);
    if (cache_data)
        return true;
    else
        return false;
}

/*
 * Creates A RangeVar for the given label.
 */
RangeVar *get_label_range_var(char *graph_name, Oid graph_oid,
                              char *label_name)
{
    char *relname;
    label_cache_data *label_cache;

    label_cache = search_label_name_graph_cache(label_name, graph_oid);

    relname = get_rel_name(label_cache->relation);

    return makeRangeVar(graph_name, relname, 2);
}

/*
 * Retrieves a list of all the names of a graph.
 *
 * XXX: We may want to use the cache system for this function,
 * however the cache system currently requires us to know the
 * name of the label we want.
 */
List *get_all_edge_labels_per_graph(EState *estate, Oid graph_oid)
{
    List *labels = NIL;
    ScanKeyData scan_keys[2];
    Relation ag_label;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    TupleTableSlot *slot;
    ResultRelInfo *resultRelInfo;

    /* setup scan keys to get all edges for the given graph oid */
    ScanKeyInit(&scan_keys[1], Anum_ag_label_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));
    ScanKeyInit(&scan_keys[0], Anum_ag_label_kind, BTEqualStrategyNumber,
                F_CHAREQ, CharGetDatum(LABEL_TYPE_EDGE));

    /* setup the table to be scanned */
    ag_label = table_open(ag_label_relation_id(), RowExclusiveLock);
    scan_desc = table_beginscan(ag_label, estate->es_snapshot, 2, scan_keys);

    resultRelInfo = create_entity_result_rel_info(estate, "ag_catalog",
                                                  "ag_label");

    slot = ExecInitExtraTupleSlot(
        estate, RelationGetDescr(resultRelInfo->ri_RelationDesc),
        &TTSOpsHeapTuple);

    /* scan through the results and get all the label names. */
    while(true)
    {
        Name label;
        bool isNull;
        Datum datum;

        tuple = heap_getnext(scan_desc, ForwardScanDirection);

        /* no more labels to process */
        if (!HeapTupleIsValid(tuple))
            break;

        ExecStoreHeapTuple(tuple, slot, false);

        datum = slot_getattr(slot, Anum_ag_label_name, &isNull);
        label = DatumGetName(datum);

        labels = lappend(labels, label);
    }

    table_endscan(scan_desc);

    destroy_entity_result_rel_info(resultRelInfo);
    table_close(resultRelInfo->ri_RelationDesc, RowExclusiveLock);

    return labels;
}

/*
 * Edge Schema Management Functions
 */

/*
 * INSERT INTO ag_catalog.ag_graph_schema
 * VALUES (graph, edge_label_id, start_label_id, end_label_id)
 *
 * This is called when a new edge is created with a previously unseen
 * (start_label, end_label) combination.
 */
void insert_edge_schema_entry(Oid graph_oid, int32 edge_label_id,
                               int32 start_label_id, int32 end_label_id)
{
    Datum values[Natts_ag_graph_schema];
    bool nulls[Natts_ag_graph_schema];
    Relation ag_graph_schema;
    HeapTuple tuple;

    Assert(OidIsValid(graph_oid));
    Assert(label_id_is_valid(edge_label_id));
    Assert(label_id_is_valid(start_label_id));
    Assert(label_id_is_valid(end_label_id));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), RowExclusiveLock);

    values[Anum_ag_graph_schema_graph - 1] = ObjectIdGetDatum(graph_oid);
    values[Anum_ag_graph_schema_edge_label_id - 1] = Int32GetDatum(edge_label_id);
    values[Anum_ag_graph_schema_start_label_id - 1] = Int32GetDatum(start_label_id);
    values[Anum_ag_graph_schema_end_label_id - 1] = Int32GetDatum(end_label_id);

    memset(nulls, false, sizeof(nulls));

    tuple = heap_form_tuple(RelationGetDescr(ag_graph_schema), values, nulls);

    CatalogTupleInsert(ag_graph_schema, tuple);

    heap_freetuple(tuple);

    table_close(ag_graph_schema, RowExclusiveLock);

    /* Invalidate all edge schema caches for this graph */
    invalidate_edge_schema_caches_for_graph(graph_oid);

    /* Make the new tuple visible for subsequent catalog lookups */
    CommandCounterIncrement();
}

/*
 * Check if an edge schema entry already exists for the given combination.
 */
bool edge_schema_entry_exists(Oid graph_oid, int32 edge_label_id,
                               int32 start_label_id, int32 end_label_id)
{
    ScanKeyData scan_keys[4];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    bool exists = false;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));
    ScanKeyInit(&scan_keys[1], Anum_ag_graph_schema_edge_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(edge_label_id));
    ScanKeyInit(&scan_keys[2], Anum_ag_graph_schema_start_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(start_label_id));
    ScanKeyInit(&scan_keys[3], Anum_ag_graph_schema_end_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(end_label_id));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), AccessShareLock);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 4, scan_keys);

    tuple = heap_getnext(scan_desc, ForwardScanDirection);
    if (HeapTupleIsValid(tuple))
        exists = true;

    table_endscan(scan_desc);
    table_close(ag_graph_schema, AccessShareLock);

    return exists;
}

/*
 * Check if a graph has any edge schema entries.
 * Used to determine if the graph has complete schema data for inference.
 * Returns true if the graph has at least one edge schema entry.
 */
bool graph_has_edge_schema_entries(Oid graph_oid)
{
    ScanKeyData scan_keys[1];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    HeapTuple tuple;
    bool has_entries = false;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), AccessShareLock);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 1, scan_keys);

    tuple = heap_getnext(scan_desc, ForwardScanDirection);
    if (HeapTupleIsValid(tuple))
        has_entries = true;

    table_endscan(scan_desc);
    table_close(ag_graph_schema, AccessShareLock);

    return has_entries;
}

/*
 * Get all schema entries for an edge label.
 * Returns an EdgeLabelSchema struct that must be freed with free_edge_label_schema().
 * Uses the edge schema cache for efficiency.
 */
EdgeLabelSchema *get_edge_label_schema(Oid graph_oid, int32 edge_label_id)
{
    edge_schema_cache_data *cached;
    EdgeLabelSchema *schema;
    int i;

    /* Get from cache */
    cached = search_edge_schema_cache(graph_oid, edge_label_id);

    /* Build the result struct from cached data */
    schema = palloc(sizeof(EdgeLabelSchema));
    schema->graph_oid = graph_oid;
    schema->edge_label_id = edge_label_id;
    schema->num_entries = cached->num_entries;

    if (schema->num_entries > 0)
    {
        schema->entries = palloc(sizeof(EdgeSchemaEntry) * schema->num_entries);
        for (i = 0; i < schema->num_entries; i++)
        {
            schema->entries[i].start_label_id = cached->entries[i].start_label_id;
            schema->entries[i].end_label_id = cached->entries[i].end_label_id;
        }
    }
    else
    {
        schema->entries = NULL;
    }

    return schema;
}

/*
 * Free an EdgeLabelSchema structure.
 */
void free_edge_label_schema(EdgeLabelSchema *schema)
{
    if (schema == NULL)
        return;

    if (schema->entries != NULL)
        pfree(schema->entries);

    pfree(schema);
}

/*
 * Get unique start label_ids for an edge label.
 * Returns a List of Int32 values. Uses the edge schema cache directly.
 */
List *get_edge_start_label_ids(Oid graph_oid, int32 edge_label_id)
{
    edge_schema_cache_data *cached;
    List *label_ids = NIL;
    int i;

    cached = search_edge_schema_cache(graph_oid, edge_label_id);

    for (i = 0; i < cached->num_entries; i++)
    {
        int32 label_id = cached->entries[i].start_label_id;

        /* Check if already in list (simple dedup) */
        if (!list_member_int(label_ids, label_id))
            label_ids = lappend_int(label_ids, label_id);
    }

    return label_ids;
}

/*
 * Get unique end label_ids for an edge label.
 * Returns a List of Int32 values. Uses the edge schema cache directly.
 */
List *get_edge_end_label_ids(Oid graph_oid, int32 edge_label_id)
{
    edge_schema_cache_data *cached;
    List *label_ids = NIL;
    int i;

    cached = search_edge_schema_cache(graph_oid, edge_label_id);

    for (i = 0; i < cached->num_entries; i++)
    {
        int32 label_id = cached->entries[i].end_label_id;

        /* Check if already in list (simple dedup) */
        if (!list_member_int(label_ids, label_id))
            label_ids = lappend_int(label_ids, label_id);
    }

    return label_ids;
}

/*
 * Get unique end label_ids for all edges that start from a specific vertex label.
 * Used when matching unlabeled edges from a labeled vertex.
 * Returns a List of Int32 values. Uses the edge schema cache.
 */
List *get_end_labels_from_start_vertex(Oid graph_oid, int32 start_vertex_label_id)
{
    vertex_edge_labels_cache_data *cached;
    List *label_ids = NIL;
    int i;

    /* Get from cache */
    cached = search_start_vertex_end_labels_cache(graph_oid, start_vertex_label_id);

    /* Convert cached array to List */
    for (i = 0; i < cached->num_label_ids; i++)
    {
        label_ids = lappend_int(label_ids, cached->label_ids[i]);
    }

    return label_ids;
}

/*
 * Get unique start label_ids for all edges that end at a specific vertex label.
 * Used when matching unlabeled edges to a labeled vertex.
 * Returns a List of Int32 values. Uses the edge schema cache.
 */
List *get_start_labels_from_end_vertex(Oid graph_oid, int32 end_vertex_label_id)
{
    vertex_edge_labels_cache_data *cached;
    List *label_ids = NIL;
    int i;

    /* Get from cache */
    cached = search_end_vertex_start_labels_cache(graph_oid, end_vertex_label_id);

    /* Convert cached array to List */
    for (i = 0; i < cached->num_label_ids; i++)
    {
        label_ids = lappend_int(label_ids, cached->label_ids[i]);
    }

    return label_ids;
}

/*
 * Get edge label_ids for edges that start from a specific vertex label.
 * Used when inferring edge labels from labeled start vertices.
 * Returns a List of Int32 edge label_ids.
 */
List *get_edge_labels_from_start_vertex(Oid graph_oid, int32 start_vertex_label_id)
{
    ScanKeyData scan_keys[2];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    TupleDesc tupdesc;
    HeapTuple tuple;
    List *edge_label_ids = NIL;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));
    ScanKeyInit(&scan_keys[1], Anum_ag_graph_schema_start_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(start_vertex_label_id));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), AccessShareLock);
    tupdesc = RelationGetDescr(ag_graph_schema);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 2, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        Datum datum;
        bool isnull;
        int32 edge_label_id;

        datum = heap_getattr(tuple, Anum_ag_graph_schema_edge_label_id,
                             tupdesc, &isnull);
        edge_label_id = DatumGetInt32(datum);

        /* Add unique edge label_ids */
        if (!list_member_int(edge_label_ids, edge_label_id))
            edge_label_ids = lappend_int(edge_label_ids, edge_label_id);
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, AccessShareLock);

    return edge_label_ids;
}

/*
 * Get edge label_ids for edges that end at a specific vertex label.
 * Used when inferring edge labels from labeled end vertices.
 * Returns a List of Int32 edge label_ids.
 */
List *get_edge_labels_from_end_vertex(Oid graph_oid, int32 end_vertex_label_id)
{
    ScanKeyData scan_keys[2];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    TupleDesc tupdesc;
    HeapTuple tuple;
    List *edge_label_ids = NIL;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));
    ScanKeyInit(&scan_keys[1], Anum_ag_graph_schema_end_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(end_vertex_label_id));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), AccessShareLock);
    tupdesc = RelationGetDescr(ag_graph_schema);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 2, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        Datum datum;
        bool isnull;
        int32 edge_label_id;

        datum = heap_getattr(tuple, Anum_ag_graph_schema_edge_label_id,
                             tupdesc, &isnull);
        edge_label_id = DatumGetInt32(datum);

        /* Add unique edge label_ids */
        if (!list_member_int(edge_label_ids, edge_label_id))
            edge_label_ids = lappend_int(edge_label_ids, edge_label_id);
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, AccessShareLock);

    return edge_label_ids;
}

/*
 * Delete all schema entries for an edge label (when dropping label).
 */
void delete_edge_schema_entries(Oid graph_oid, int32 edge_label_id)
{
    ScanKeyData scan_keys[2];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    HeapTuple tuple;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));
    ScanKeyInit(&scan_keys[1], Anum_ag_graph_schema_edge_label_id, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(edge_label_id));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), RowExclusiveLock);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 2, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        CatalogTupleDelete(ag_graph_schema, &tuple->t_self);
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, RowExclusiveLock);

    /* Invalidate all edge schema caches for this graph */
    invalidate_edge_schema_caches_for_graph(graph_oid);
}

/*
 * Delete all schema entries for a graph (when dropping graph).
 */
void delete_edge_schema_entries_for_graph(Oid graph_oid)
{
    ScanKeyData scan_keys[1];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    HeapTuple tuple;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), RowExclusiveLock);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 1, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        CatalogTupleDelete(ag_graph_schema, &tuple->t_self);
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, RowExclusiveLock);

    /* Invalidate all edge schema caches for this graph */
    invalidate_edge_schema_caches_for_graph(graph_oid);
}

/*
 * Delete all schema entries referencing a vertex label (when dropping vertex label).
 * This removes entries where start_label_id OR end_label_id matches the vertex label.
 */
void delete_edge_schema_entries_for_vertex(Oid graph_oid, int32 vertex_label_id)
{
    ScanKeyData scan_keys[1];
    Relation ag_graph_schema;
    TableScanDesc scan_desc;
    TupleDesc tupdesc;
    HeapTuple tuple;

    ScanKeyInit(&scan_keys[0], Anum_ag_graph_schema_graph, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(graph_oid));

    ag_graph_schema = table_open(ag_graph_schema_relation_id(), RowExclusiveLock);
    tupdesc = RelationGetDescr(ag_graph_schema);
    scan_desc = table_beginscan_catalog(ag_graph_schema, 1, scan_keys);

    while ((tuple = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
    {
        Datum start_datum, end_datum;
        bool start_isnull, end_isnull;
        int32 start_label_id, end_label_id;

        start_datum = heap_getattr(tuple, Anum_ag_graph_schema_start_label_id,
                                   tupdesc, &start_isnull);
        end_datum = heap_getattr(tuple, Anum_ag_graph_schema_end_label_id,
                                 tupdesc, &end_isnull);

        start_label_id = DatumGetInt32(start_datum);
        end_label_id = DatumGetInt32(end_datum);

        /* Delete if this entry references the dropped vertex label */
        if (start_label_id == vertex_label_id || end_label_id == vertex_label_id)
        {
            CatalogTupleDelete(ag_graph_schema, &tuple->t_self);
        }
    }

    table_endscan(scan_desc);
    table_close(ag_graph_schema, RowExclusiveLock);

    /* Invalidate all edge schema caches for this graph */
    invalidate_edge_schema_caches_for_graph(graph_oid);
}

/*
 * Convert label_id to relation OID.
 * Looks up the relation for a given (graph_oid, label_id) combination.
 * Uses the label cache for efficiency.
 */
Oid label_id_to_relation(Oid graph_oid, int32 label_id)
{
    label_cache_data *cache_data;

    cache_data = search_label_graph_oid_cache(graph_oid, label_id);
    if (cache_data)
        return cache_data->relation;

    return InvalidOid;
}
