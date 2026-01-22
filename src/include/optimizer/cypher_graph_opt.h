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

#ifndef AG_CYPHER_GRAPH_OPT_H
#define AG_CYPHER_GRAPH_OPT_H

#include "postgres.h"
#include "nodes/pg_list.h"

/*
 * Graph-aware optimization context
 *
 * This structure stores optimization hints that are determined during parsing
 * and need to be passed to the planner. The primary use case is label inference
 * - when we can determine which vertex labels are possible based on adjacent
 * edge labels, we can limit the tables scanned in an Append operation.
 *
 * The context uses the variable name (alias) as the key, which uniquely
 * identifies each vertex/edge in the query. The value is a list of
 * child table OIDs that should be scanned.
 */

/* Entry in the inferred labels map */
typedef struct InferredLabelEntry
{
    char *var_name;         /* Variable name (alias) as key */
    List *child_relids;     /* List of allowed child table OIDs */
} InferredLabelEntry;

/*
 * Initialize/cleanup the graph optimization context for current query.
 * Should be called at the start/end of query processing.
 */
extern void graph_opt_context_init(void);
extern void graph_opt_context_cleanup(void);

/*
 * Register inferred labels for a variable (vertex or edge).
 * This is called from the parser when we infer that only certain labels
 * are possible for an unlabeled vertex/edge based on edge schema.
 *
 * var_name: The variable name (alias) identifying this vertex/edge
 * label_relids: List of Oids of the specific label tables to scan
 */
extern void graph_opt_add_inferred_labels(const char *var_name, List *label_relids);

/*
 * Check if there are inferred labels for a given variable name.
 * Returns the list of allowed child OIDs, or NIL if no inference was done.
 *
 * var_name: The variable name (alias) to check
 */
extern List *graph_opt_get_inferred_labels(const char *var_name);

/*
 * Check if an inferred label entry exists for the variable (even if empty).
 * Used to detect impossible patterns where no labels are valid.
 */
extern bool graph_opt_has_inferred_entry(const char *var_name);


#endif /* AG_CYPHER_GRAPH_OPT_H */
