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

#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/lsyscache.h"

#include "optimizer/cypher_graph_opt.h"
#include "optimizer/cypher_pathnode.h"
#include "optimizer/cypher_paths.h"
#include "utils/ag_func.h"
#include "utils/ag_guc.h"

typedef enum cypher_clause_kind
{
    CYPHER_CLAUSE_NONE,
    CYPHER_CLAUSE_CREATE,
    CYPHER_CLAUSE_SET,
    CYPHER_CLAUSE_DELETE,
    CYPHER_CLAUSE_MERGE
} cypher_clause_kind;

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static join_search_hook_type prev_join_search_hook;

static void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
                             RangeTblEntry *rte);
static cypher_clause_kind get_cypher_clause_kind(RangeTblEntry *rte);
static void handle_cypher_create_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte);
static void handle_cypher_set_clause(PlannerInfo *root, RelOptInfo *rel,
                                     Index rti, RangeTblEntry *rte);
static void handle_cypher_delete_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte);
static void handle_cypher_merge_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte);
static void filter_append_paths_for_inferred_labels(PlannerInfo *root,
                                                    RelOptInfo *rel,
                                                    RangeTblEntry *rte);
static RelOptInfo *age_join_search(PlannerInfo *root, int levels_needed,
                                   List *initial_rels);
static void update_rel_rows_after_filtering(PlannerInfo *root,
                                            List *initial_rels);

void set_rel_pathlist_init(void)
{
    prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = set_rel_pathlist;

    prev_join_search_hook = join_search_hook;
    join_search_hook = age_join_search;
}

void set_rel_pathlist_fini(void)
{
    set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
    join_search_hook = prev_join_search_hook;
}

static void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
                             RangeTblEntry *rte)
{
    if (prev_set_rel_pathlist_hook)
        prev_set_rel_pathlist_hook(root, rel, rti, rte);

    switch (get_cypher_clause_kind(rte))
    {
    case CYPHER_CLAUSE_CREATE:
        handle_cypher_create_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_SET:
        handle_cypher_set_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_DELETE:
        handle_cypher_delete_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_MERGE:
        handle_cypher_merge_clause(root, rel, rti, rte);
        break;
    case CYPHER_CLAUSE_NONE:
        /*
         * For regular cypher queries, apply inferred label filtering
         * to prune AppendPath children based on graph schema inference.
         * This optimization can be disabled via: SET age.infer_labels = off;
         */
        if (age_infer_labels)
            filter_append_paths_for_inferred_labels(root, rel, rte);
        break;
    default:
        ereport(ERROR, (errmsg_internal("invalid cypher_clause_kind")));
    }
}

/*
 * Check to see if the rte is a Cypher clause. An rte is only a Cypher clause
 * if it is a subquery, with the last entry in its target list, that is a
 * FuncExpr.
 */
static cypher_clause_kind get_cypher_clause_kind(RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;

    /* If it's not a subquery, it's not a Cypher clause. */
    if (rte->rtekind != RTE_SUBQUERY)
        return CYPHER_CLAUSE_NONE;

    /* Make sure the targetList isn't NULL. NULL means potential EXIST subclause */
    if (rte->subquery->targetList == NULL)
        return CYPHER_CLAUSE_NONE;

    /* A Cypher clause function is always the last entry. */
    te = llast(rte->subquery->targetList);

    /* If the last entry is not a FuncExpr, it's not a Cypher clause. */
    if (!IsA(te->expr, FuncExpr))
        return CYPHER_CLAUSE_NONE;

    fe = (FuncExpr *)te->expr;

    if (is_oid_ag_func(fe->funcid, CREATE_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_CREATE;
    if (is_oid_ag_func(fe->funcid, SET_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_SET;
    if (is_oid_ag_func(fe->funcid, DELETE_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_DELETE;
    if (is_oid_ag_func(fe->funcid, MERGE_CLAUSE_FUNCTION_NAME))
        return CYPHER_CLAUSE_MERGE;
    else
        return CYPHER_CLAUSE_NONE;
}

/* replace all possible paths with our CustomPath */
static void handle_cypher_delete_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    /* Add the pattern to the CustomPath */
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    /* pass the const that holds the data structure to the path. */
    custom_private = fe->args;

    cp = create_cypher_delete_path(root, rel, custom_private);

    /* Discard any preexisting paths */
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    add_path(rel, (Path *)cp);
}

/*
 * Take the paths possible for the RelOptInfo that represents our
 * _cypher_delete_clause function replace them with our delete clause
 * path. The original paths will be children to the new delete path.
 */
static void handle_cypher_create_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    /* Add the pattern to the CustomPath */
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    /* pass the const that holds the data structure to the path. */
    custom_private = fe->args;

    cp = create_cypher_create_path(root, rel, custom_private);

    /* Discard any preexisting paths, they should be under the cp path */
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    /* Add the new path to the rel. */
    add_path(rel, (Path *)cp);
}

/* replace all possible paths with our CustomPath */
static void handle_cypher_set_clause(PlannerInfo *root, RelOptInfo *rel,
                                     Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    /* Add the pattern to the CustomPath */
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    /* pass the const that holds the data structure to the path. */
    custom_private = fe->args;

    cp = create_cypher_set_path(root, rel, custom_private);

    /* Discard any preexisting paths */
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    add_path(rel, (Path *)cp);
}

/* replace all possible paths with our CustomPath */
static void handle_cypher_merge_clause(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte)
{
    TargetEntry *te;
    FuncExpr *fe;
    List *custom_private;
    CustomPath *cp;

    /* Add the pattern to the CustomPath */
    te = (TargetEntry *)llast(rte->subquery->targetList);
    fe = (FuncExpr *)te->expr;
    /* pass the const that holds the data structure to the path. */
    custom_private = fe->args;

    cp = create_cypher_merge_path(root, rel, custom_private);

    /* Discard any preexisting paths */
    rel->pathlist = NIL;
    rel->partial_pathlist = NIL;

    add_path(rel, (Path *)cp);
}

/*
 * filter_append_paths_for_inferred_labels
 *
 * When we have inferred that only certain vertex labels are possible for
 * an unlabeled vertex based on edge schema, we can optimize the query plan
 * by filtering the AppendPath to only scan those labels.
 *
 * This function checks if the relation is an inheritance parent (like
 * _ag_label_vertex) with registered inferred labels. If so, it filters
 * the subpaths in all AppendPaths to only include allowed children.
 */
static void filter_append_paths_for_inferred_labels(PlannerInfo *root,
                                                    RelOptInfo *rel,
                                                    RangeTblEntry *rte)
{
    List *inferred_labels;
    ListCell *lc;
    char *rel_name;
    const char *var_name;

    /* Only process relation RTEs with inheritance */
    if (rte->rtekind != RTE_RELATION || !rte->inh)
        return;

    /*
     * Only apply this optimization to AGE's base vertex/edge tables.
     * These are the parent tables that use inheritance for label hierarchy.
     */
    rel_name = get_rel_name(rte->relid);
    if (rel_name == NULL)
        return;

    if (strcmp(rel_name, "_ag_label_vertex") != 0 &&
        strcmp(rel_name, "_ag_label_edge") != 0)
    {
        pfree(rel_name);
        return;
    }
    pfree(rel_name);

    /*
     * Get the variable name from the RTE alias.
     * The parser sets the alias to the Cypher variable name.
     */
    if (rte->alias == NULL || rte->alias->aliasname == NULL)
        return;
    
    var_name = rte->alias->aliasname;

    /* 
     * Check if there are inferred labels for this variable.
     * If an entry exists but is empty, it means the pattern is impossible
     * (no edges/vertices can satisfy the constraints). We still need to
     * handle this case to add a one-time false filter.
     */
    if (!graph_opt_has_inferred_entry(var_name))
        return;
    
    inferred_labels = graph_opt_get_inferred_labels(var_name);
    
    /*
     * If inferred_labels is NIL (empty) but an entry exists, it means
     * the pattern is impossible. Replace all paths with a Result node
     * that returns no rows (one-time filter: false).
     */
    if (inferred_labels == NIL)
    {
        elog(DEBUG1, "filter_append_paths: impossible pattern for variable '%s' - adding false filter",
             var_name);
        
        /* 
         * Replace all paths with a single path that returns no rows.
         * We create a dummy path with 0 rows and mark it with a qual
         * that is always false. The actual implementation uses the
         * existing paths but the planner should eventually prune them.
         *
         * For now, we simply remove all subpaths from Append/MergeAppend
         * which effectively returns no rows.
         */
        foreach(lc, rel->pathlist)
        {
            Path *path = (Path *) lfirst(lc);

            if (IsA(path, AppendPath))
            {
                AppendPath *append_path = (AppendPath *) path;
                list_free(append_path->subpaths);
                append_path->subpaths = NIL;
                append_path->path.rows = 0;
                append_path->path.startup_cost = 0;
                append_path->path.total_cost = 0;
            }
            else if (IsA(path, MergeAppendPath))
            {
                MergeAppendPath *merge_append_path = (MergeAppendPath *) path;
                list_free(merge_append_path->subpaths);
                merge_append_path->subpaths = NIL;
                merge_append_path->path.rows = 0;
                merge_append_path->path.startup_cost = 0;
                merge_append_path->path.total_cost = 0;
            }
        }
        
        foreach(lc, rel->partial_pathlist)
        {
            Path *path = (Path *) lfirst(lc);

            if (IsA(path, AppendPath))
            {
                AppendPath *append_path = (AppendPath *) path;
                list_free(append_path->subpaths);
                append_path->subpaths = NIL;
                append_path->path.rows = 0;
            }
            else if (IsA(path, MergeAppendPath))
            {
                MergeAppendPath *merge_append_path = (MergeAppendPath *) path;
                list_free(merge_append_path->subpaths);
                merge_append_path->subpaths = NIL;
                merge_append_path->path.rows = 0;
            }
        }
        return;
    }

    elog(DEBUG1, "filter_append_paths: filtering paths for variable '%s' with %d inferred labels",
         var_name, list_length(inferred_labels));

    /*
     * Iterate through all paths and filter AppendPaths and MergeAppendPaths
     * in-place. We modify the subpaths list directly rather than creating
     * new paths to avoid issues with parameterization requirements.
     *
     * Note: PostgreSQL generates both AppendPath (unordered) and MergeAppendPath
     * (ordered, for merge joins). We must filter both, otherwise the planner
     * may choose an unfiltered MergeAppendPath.
     */
    foreach(lc, rel->pathlist)
    {
        Path *path = (Path *) lfirst(lc);

        if (IsA(path, AppendPath))
        {
            AppendPath *append_path = (AppendPath *) path;
            List *filtered_subpaths = NIL;
            ListCell *sublc;
            int original_count = list_length(append_path->subpaths);
            int filtered_count = 0;
            Cost total_cost = 0;
            Cost startup_cost = 0;
            double total_rows = 0;

            /* Filter subpaths to only include allowed children */
            foreach(sublc, append_path->subpaths)
            {
                Path *subpath = (Path *) lfirst(sublc);
                Index child_rti = subpath->parent->relid;
                RangeTblEntry *child_rte = root->simple_rte_array[child_rti];

                /*
                 * Check if this child is in the allowed list.
                 * The child's relid should be in our inferred_labels list.
                 *
                 * Note: We do NOT skip the parent table (_ag_label_vertex or
                 * _ag_label_edge) when it's in the inferred_labels list. This
                 * is because unlabeled vertices are actually stored in
                 * _ag_label_vertex (label_id=1), and the edge schema records
                 * edges from these unlabeled vertices. So if label_id=1 is
                 * inferred, we must include the _ag_label_vertex table scan.
                 */
                if (child_rte != NULL &&
                    child_rte->rtekind == RTE_RELATION &&
                    list_member_oid(inferred_labels, child_rte->relid))
                {
                    filtered_subpaths = lappend(filtered_subpaths, subpath);
                    filtered_count++;

                    /* Accumulate costs */
                    total_cost += subpath->total_cost;
                    total_rows += subpath->rows;
                }
            }

            elog(DEBUG1, "filter_append_paths: filtered AppendPath %d -> %d subpaths",
                 original_count, filtered_count);

            /*
             * If we filtered any subpaths, update the AppendPath in place.
             *
             * Note: We intentionally keep AppendPath even with single child.
             * Unwrapping would require remapping all Var references from child
             * to parent relation, which is complex and error-prone.
             */
            if (filtered_count > 0 && filtered_count < original_count)
            {
                ListCell *first_lc;

                /* Update subpaths list */
                append_path->subpaths = filtered_subpaths;

                /*
                 * Recalculate costs following PostgreSQL's cost_append() logic:
                 * - For unordered Append: startup = first child's startup
                 * - For ordered Append (has pathkeys): startup = sum of startups
                 */
                if (append_path->path.pathkeys == NIL)
                {
                    /* Unordered: startup = first subpath's startup */
                    Path *first_subpath = (Path *) linitial(filtered_subpaths);
                    startup_cost = first_subpath->startup_cost;
                }
                else
                {
                    /* Ordered: startup = sum of all subpath startups */
                    startup_cost = 0;
                    foreach(first_lc, filtered_subpaths)
                    {
                        Path *subpath = (Path *) lfirst(first_lc);
                        startup_cost += subpath->startup_cost;
                    }
                }

                append_path->path.startup_cost = startup_cost;
                append_path->path.total_cost = total_cost;
                append_path->path.rows = total_rows;
            }
        }
        else if (IsA(path, MergeAppendPath))
        {
            MergeAppendPath *merge_path = (MergeAppendPath *) path;
            List *filtered_subpaths = NIL;
            ListCell *sublc;
            int original_count = list_length(merge_path->subpaths);
            int filtered_count = 0;
            Cost total_cost = 0;
            Cost startup_cost = 0;
            double total_rows = 0;

            /* Filter subpaths to only include allowed children */
            foreach(sublc, merge_path->subpaths)
            {
                Path *subpath = (Path *) lfirst(sublc);
                Index child_rti = subpath->parent->relid;
                RangeTblEntry *child_rte = root->simple_rte_array[child_rti];

                if (child_rte != NULL &&
                    child_rte->rtekind == RTE_RELATION &&
                    list_member_oid(inferred_labels, child_rte->relid))
                {
                    filtered_subpaths = lappend(filtered_subpaths, subpath);
                    filtered_count++;

                    /* Accumulate costs */
                    total_cost += subpath->total_cost;
                    total_rows += subpath->rows;
                    startup_cost += subpath->startup_cost;
                }
            }

            elog(DEBUG1, "filter_append_paths: filtered MergeAppendPath %d -> %d subpaths",
                 original_count, filtered_count);

            /*
             * If we filtered any subpaths, update the MergeAppendPath in place.
             */
            if (filtered_count > 0 && filtered_count < original_count)
            {
                /* Update subpaths list */
                merge_path->subpaths = filtered_subpaths;

                /*
                 * For MergeAppend, the startup cost is the sum of all
                 * subpath startup costs (need all to start producing).
                 */
                merge_path->path.startup_cost = startup_cost;
                merge_path->path.total_cost = total_cost;
                merge_path->path.rows = total_rows;
            }
        }
    }

    /*
     * Also filter partial_pathlist for parallel query support.
     */
    foreach(lc, rel->partial_pathlist)
    {
        Path *path = (Path *) lfirst(lc);

        if (IsA(path, AppendPath))
        {
            AppendPath *append_path = (AppendPath *) path;
            List *filtered_subpaths = NIL;
            ListCell *sublc;
            int original_count = list_length(append_path->subpaths);
            int filtered_count = 0;
            Cost total_cost = 0;
            double total_rows = 0;

            foreach(sublc, append_path->subpaths)
            {
                Path *subpath = (Path *) lfirst(sublc);
                Index child_rti = subpath->parent->relid;
                RangeTblEntry *child_rte = root->simple_rte_array[child_rti];

                if (child_rte != NULL &&
                    child_rte->rtekind == RTE_RELATION &&
                    list_member_oid(inferred_labels, child_rte->relid))
                {
                    filtered_subpaths = lappend(filtered_subpaths, subpath);
                    filtered_count++;
                    total_cost += subpath->total_cost;
                    total_rows += subpath->rows;
                }
            }

            if (filtered_count > 0 && filtered_count < original_count)
            {
                append_path->subpaths = filtered_subpaths;
                append_path->path.total_cost = total_cost;
                append_path->path.rows = total_rows;
            }
        }
        else if (IsA(path, MergeAppendPath))
        {
            MergeAppendPath *merge_path = (MergeAppendPath *) path;
            List *filtered_subpaths = NIL;
            ListCell *sublc;
            int original_count = list_length(merge_path->subpaths);
            int filtered_count = 0;
            Cost total_cost = 0;
            Cost startup_cost = 0;
            double total_rows = 0;

            foreach(sublc, merge_path->subpaths)
            {
                Path *subpath = (Path *) lfirst(sublc);
                Index child_rti = subpath->parent->relid;
                RangeTblEntry *child_rte = root->simple_rte_array[child_rti];

                if (child_rte != NULL &&
                    child_rte->rtekind == RTE_RELATION &&
                    list_member_oid(inferred_labels, child_rte->relid))
                {
                    filtered_subpaths = lappend(filtered_subpaths, subpath);
                    filtered_count++;
                    total_cost += subpath->total_cost;
                    total_rows += subpath->rows;
                    startup_cost += subpath->startup_cost;
                }
            }

            if (filtered_count > 0 && filtered_count < original_count)
            {
                merge_path->subpaths = filtered_subpaths;
                merge_path->path.startup_cost = startup_cost;
                merge_path->path.total_cost = total_cost;
                merge_path->path.rows = total_rows;
            }
        }
    }

    /* Recalculate cheapest paths after modification */
    set_cheapest(rel);
}

/*
 * update_rel_rows_after_filtering
 *
 * After filtering AppendPaths for inferred labels, the rel->rows estimate
 * may be stale (it was computed before filtering). This function updates
 * rel->rows to match the cheapest path's rows estimate, ensuring that
 * join ordering uses accurate cardinality estimates.
 *
 * This is critical for optimal join ordering because PostgreSQL's join
 * search algorithm uses rel->rows for cardinality estimation when deciding
 * join order and join methods.
 */
static void
update_rel_rows_after_filtering(PlannerInfo *root, List *initial_rels)
{
    ListCell *lc;

    foreach(lc, initial_rels)
    {
        RelOptInfo *rel = (RelOptInfo *) lfirst(lc);
        RangeTblEntry *rte;
        char *rel_name;
        const char *var_name;
        Path *cheapest_path;

        /* Skip if not a base relation */
        if (rel->reloptkind != RELOPT_BASEREL)
            continue;

        /* Get the RTE for this relation */
        if (rel->relid >= root->simple_rel_array_size)
            continue;

        rte = root->simple_rte_array[rel->relid];
        if (rte == NULL || rte->rtekind != RTE_RELATION || !rte->inh)
            continue;

        /* Only process AGE's base vertex/edge tables */
        rel_name = get_rel_name(rte->relid);
        if (rel_name == NULL)
            continue;

        if (strcmp(rel_name, "_ag_label_vertex") != 0 &&
            strcmp(rel_name, "_ag_label_edge") != 0)
        {
            pfree(rel_name);
            continue;
        }
        pfree(rel_name);

        /* Get the variable name from the alias */
        if (rte->alias == NULL || rte->alias->aliasname == NULL)
            continue;

        var_name = rte->alias->aliasname;

        /* Check if this variable has inferred labels */
        if (!graph_opt_has_inferred_entry(var_name))
            continue;

        /*
         * Update rel->rows to match the cheapest total path's rows.
         * This ensures join ordering uses the filtered cardinality.
         */
        cheapest_path = rel->cheapest_total_path;
        if (cheapest_path != NULL && cheapest_path->rows != rel->rows)
        {
            elog(DEBUG1, "age_join_search: updating rel->rows for '%s' from %.0f to %.0f",
                 var_name, rel->rows, cheapest_path->rows);

            rel->rows = cheapest_path->rows;
        }
    }
}

/*
 * age_join_search
 *
 * Custom join_search_hook that ensures accurate cardinality estimates
 * after AppendPath filtering for inferred labels.
 *
 * The problem: PostgreSQL computes rel->rows in set_base_rel_sizes() before
 * paths are created. Our set_rel_pathlist_hook filters AppendPaths based on
 * inferred labels, but rel->rows still reflects the original (unfiltered)
 * estimate. This leads to suboptimal join ordering.
 *
 * The solution: Before calling standard_join_search(), we update rel->rows
 * for all filtered relations to match their cheapest path's rows estimate.
 * This ensures the join search algorithm uses accurate cardinality estimates.
 */
static RelOptInfo *
age_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
    /*
     * Update rel->rows for relations that were filtered based on
     * inferred labels. This must happen before join ordering.
     */
    if (age_infer_labels)
    {
        update_rel_rows_after_filtering(root, initial_rels);
    }

    /*
     * Call the previous hook if one exists, otherwise use standard_join_search.
     */
    if (prev_join_search_hook)
        return prev_join_search_hook(root, levels_needed, initial_rels);
    else
        return standard_join_search(root, levels_needed, initial_rels);
}
