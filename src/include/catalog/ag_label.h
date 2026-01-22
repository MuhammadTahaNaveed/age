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

#ifndef AG_AG_LABEL_H
#define AG_AG_LABEL_H

#include "nodes/execnodes.h"

#define Anum_ag_label_vertex_table_id 1
#define Anum_ag_label_vertex_table_properties 2

#define Anum_ag_label_edge_table_id 1
#define Anum_ag_label_edge_table_start_id 2
#define Anum_ag_label_edge_table_end_id 3
#define Anum_ag_label_edge_table_properties 4

#define vertex_tuple_id Anum_ag_label_vertex_table_id - 1
#define vertex_tuple_properties Anum_ag_label_vertex_table_properties - 1

#define edge_tuple_id Anum_ag_label_edge_table_id - 1
#define edge_tuple_start_id Anum_ag_label_edge_table_start_id - 1
#define edge_tuple_end_id Anum_ag_label_edge_table_end_id - 1
#define edge_tuple_properties Anum_ag_label_edge_table_properties - 1



#define Anum_ag_label_name 1
#define Anum_ag_label_graph 2
#define Anum_ag_label_id 3
#define Anum_ag_label_kind 4
#define Anum_ag_label_relation 5
#define Anum_ag_label_seq_name 6


#define Natts_ag_label 6

#define ag_label_relation_id() ag_relation_id("ag_label", "table")
#define ag_label_name_graph_index_id() \
    ag_relation_id("ag_label_name_graph_index", "index")
#define ag_label_graph_oid_index_id() \
    ag_relation_id("ag_label_graph_oid_index", "index")
#define ag_label_relation_index_id() \
    ag_relation_id("ag_label_relation_index", "index")
#define ag_label_seq_name_graph_index_id() \
    ag_relation_id("ag_label_seq_name_graph_index", "index")

#define LABEL_ID_SEQ_NAME "_label_id_seq"

#define LABEL_KIND_VERTEX 'v'
#define LABEL_KIND_EDGE 'e'

/*
 * ag_graph_schema catalog table column numbers
 */
#define Anum_ag_graph_schema_graph 1
#define Anum_ag_graph_schema_edge_label_id 2
#define Anum_ag_graph_schema_start_label_id 3
#define Anum_ag_graph_schema_end_label_id 4

#define Natts_ag_graph_schema 4

#define ag_graph_schema_relation_id() ag_relation_id("ag_graph_schema", "table")
#define ag_graph_schema_pkey_id() \
    ag_relation_id("ag_graph_schema_pkey", "index")
#define ag_graph_schema_edge_idx_id() \
    ag_relation_id("ag_graph_schema_edge_idx", "index")

/*
 * Edge schema entry - represents one valid (start, edge, end) combination.
 * Uses label_id (int32) which is encoded in graphid's upper 16 bits.
 */
typedef struct EdgeSchemaEntry
{
    int32   start_label_id;   /* Start vertex label id */
    int32   end_label_id;     /* End vertex label id */
} EdgeSchemaEntry;

/*
 * Complete edge schema for one edge label - contains all valid
 * (start, end) vertex label combinations for this edge label.
 */
typedef struct EdgeLabelSchema
{
    Oid     graph_oid;        /* Graph OID (for relation lookups) */
    int32   edge_label_id;    /* Edge label id */
    int     num_entries;      /* Number of (start, end) pairs */
    EdgeSchemaEntry *entries; /* Array of valid (start, end) pairs */
} EdgeLabelSchema;

void insert_label(const char *label_name, Oid graph_oid, int32 label_id,
                  char label_kind, Oid label_relation, const char *seq_name);
void delete_label(Oid relation);

int32 get_label_id(const char *label_name, Oid graph_oid);
Oid get_label_relation(const char *label_name, Oid graph_oid);
char *get_label_relation_name(const char *label_name, Oid graph_oid);
char get_label_kind(const char *label_name, Oid label_graph);
char *get_label_seq_relation_name(const char *label_name);


bool label_id_exists(Oid graph_oid, int32 label_id);
RangeVar *get_label_range_var(char *graph_name, Oid graph_oid,
                              char *label_name);

List *get_all_edge_labels_per_graph(EState *estate, Oid graph_oid);

/*
 * Edge schema management functions
 */

/* Insert a new edge schema entry (start_label_id, edge_label_id, end_label_id) */
void insert_edge_schema_entry(Oid graph_oid, int32 edge_label_id,
                               int32 start_label_id, int32 end_label_id);

/* Check if an edge schema entry already exists */
bool edge_schema_entry_exists(Oid graph_oid, int32 edge_label_id,
                               int32 start_label_id, int32 end_label_id);

/* Check if a graph has any edge schema entries (for inference optimization) */
bool graph_has_edge_schema_entries(Oid graph_oid);

/* Get all schema entries for an edge label */
EdgeLabelSchema *get_edge_label_schema(Oid graph_oid, int32 edge_label_id);

/* Free an EdgeLabelSchema structure */
void free_edge_label_schema(EdgeLabelSchema *schema);

/* Get unique start label_ids for an edge label */
List *get_edge_start_label_ids(Oid graph_oid, int32 edge_label_id);

/* Get unique end label_ids for an edge label */
List *get_edge_end_label_ids(Oid graph_oid, int32 edge_label_id);

/* Get end labels for all edges starting from a specific vertex label */
List *get_end_labels_from_start_vertex(Oid graph_oid, int32 start_vertex_label_id);

/* Get start labels for all edges ending at a specific vertex label */
List *get_start_labels_from_end_vertex(Oid graph_oid, int32 end_vertex_label_id);

/* Get edge labels for edges starting from a specific vertex label */
List *get_edge_labels_from_start_vertex(Oid graph_oid, int32 start_vertex_label_id);

/* Get edge labels for edges ending at a specific vertex label */
List *get_edge_labels_from_end_vertex(Oid graph_oid, int32 end_vertex_label_id);

/* Delete all schema entries for an edge label (when dropping edge label) */
void delete_edge_schema_entries(Oid graph_oid, int32 edge_label_id);

/* Delete all schema entries referencing a vertex label (when dropping vertex label) */
void delete_edge_schema_entries_for_vertex(Oid graph_oid, int32 vertex_label_id);

/* Delete all schema entries for a graph (when dropping graph) */
void delete_edge_schema_entries_for_graph(Oid graph_oid);

/* Convert label_id to relation OID */
Oid label_id_to_relation(Oid graph_oid, int32 label_id);

#define label_exists(label_name, label_graph) \
    OidIsValid(get_label_id(label_name, label_graph))

#endif
