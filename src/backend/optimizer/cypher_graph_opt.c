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

#include "utils/memutils.h"

#include "optimizer/cypher_graph_opt.h"

/*
 * Thread-local context for graph optimization hints.
 * This is reset at the start of each query.
 */
static List *inferred_label_entries = NIL;
static MemoryContext graph_opt_context = NULL;

/*
 * Initialize the graph optimization context for the current query.
 */
void
graph_opt_context_init(void)
{
    /* Create a memory context for optimization data if it doesn't exist */
    if (graph_opt_context == NULL)
    {
        graph_opt_context = AllocSetContextCreate(TopMemoryContext,
                                                   "AGE Graph Optimization",
                                                   ALLOCSET_DEFAULT_SIZES);
    }
    else
    {
        /* Reset the memory context to free all allocations */
        MemoryContextReset(graph_opt_context);
    }

    /* Clear the entries list */
    inferred_label_entries = NIL;
}

/*
 * Cleanup the graph optimization context.
 */
void
graph_opt_context_cleanup(void)
{
    if (graph_opt_context != NULL)
    {
        MemoryContextReset(graph_opt_context);
        inferred_label_entries = NIL;
    }
}

/*
 * Find an existing entry for the given variable name, or return NULL.
 */
static InferredLabelEntry *
find_inferred_entry(const char *var_name)
{
    ListCell *lc;

    if (var_name == NULL)
        return NULL;

    foreach(lc, inferred_label_entries)
    {
        InferredLabelEntry *entry = (InferredLabelEntry *) lfirst(lc);
        if (entry->var_name != NULL && strcmp(entry->var_name, var_name) == 0)
            return entry;
    }
    return NULL;
}

/*
 * Register inferred labels for a variable (vertex or edge).
 */
void
graph_opt_add_inferred_labels(const char *var_name, List *label_relids)
{
    MemoryContext old_context;
    InferredLabelEntry *entry;

    /* Skip anonymous (unnamed) vertices/edges */
    if (var_name == NULL || var_name[0] == '\0')
        return;

    if (graph_opt_context == NULL)
    {
        graph_opt_context_init();
    }

    old_context = MemoryContextSwitchTo(graph_opt_context);

    /* Check if we already have an entry for this variable */
    entry = find_inferred_entry(var_name);
    
    if (entry != NULL)
    {
        /*
         * If we already have an entry, intersect the lists.
         * This handles cases where the same variable is constrained
         * multiple times from different inference sources.
         */
        List *intersection = NIL;
        ListCell *lc;

        foreach(lc, entry->child_relids)
        {
            Oid child_oid = lfirst_oid(lc);
            if (list_member_oid(label_relids, child_oid))
            {
                intersection = lappend_oid(intersection, child_oid);
            }
        }
        list_free(entry->child_relids);
        entry->child_relids = intersection;
    }
    else
    {
        /* Create a new entry */
        entry = (InferredLabelEntry *) palloc(sizeof(InferredLabelEntry));
        entry->var_name = pstrdup(var_name);
        entry->child_relids = list_copy(label_relids);
        inferred_label_entries = lappend(inferred_label_entries, entry);
    }

    MemoryContextSwitchTo(old_context);

    elog(DEBUG1, "graph_opt: registered %d inferred labels for variable '%s'",
         list_length(entry->child_relids), var_name);
}

/*
 * Get the list of inferred label OIDs for a variable name.
 * Returns NIL if no inference was done.
 */
List *
graph_opt_get_inferred_labels(const char *var_name)
{
    InferredLabelEntry *entry;
    
    if (var_name == NULL)
        return NIL;
    
    entry = find_inferred_entry(var_name);
    
    if (entry != NULL)
        return entry->child_relids;
    
    return NIL;
}

/*
 * Check if an entry exists for the variable (even if empty).
 * This is used to detect impossible patterns where we have an
 * entry with an empty list (no valid labels).
 */
bool
graph_opt_has_inferred_entry(const char *var_name)
{
    if (var_name == NULL)
        return false;
    
    return (find_inferred_entry(var_name) != NULL);
}
