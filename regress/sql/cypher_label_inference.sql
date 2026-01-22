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

LOAD 'age';
SET search_path TO ag_catalog;

--
-- Label Inference Tests
--
SELECT create_graph('label_inference');

SELECT * FROM cypher('label_inference', $$
    CREATE (:node1 {id: 1})-[:edge1]->(:node2 {id: 2})-[:edge2]->(:node3 {id: 3})-[:edge3]->(:node4 {id: 4})-[:edge4]->(:node5 {id: 5})
$$) AS (a agtype);

-- Verify schema entries
SELECT * FROM ag_catalog.ag_graph_schema 
WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = 'label_inference')
ORDER BY edge_label_id, start_label_id, end_label_id;

--
-- Test 1: Single edge pattern
--

-- 1.1: Start vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

-- 1.2: End vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e]->(b:node2) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e]->(b:node2) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

-- 1.3: Both vertices labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e]->(b:node2) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e]->(b:node2) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

-- 1.4: Edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e:edge1]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e:edge1]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

--
-- Test 2: Single edge pattern - backward direction
--
-- 2.1: Start vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node2)<-[e]-(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node2)<-[e]-(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

-- 2.2: End vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e]-(b:node1) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e]-(b:node1) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

-- 2.3: Both vertices labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node2)<-[e]-(b:node1) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node2)<-[e]-(b:node1) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

-- 2.4: Edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e:edge1]-(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e:edge1]-(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

--
-- Test 3: Two-hop patterns - forward direction
--

-- 3.1: Labeled start vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e1]->(b)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e1]->(b)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 3.2: Labeled end vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b)-[e2]->(c:node3) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b)-[e2]->(c:node3) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 3.3: Labeled middle vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b:node2)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b:node2)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 3.4: First edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1:edge1]->(b)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1:edge1]->(b)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 3.5: Last edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b)-[e2:edge2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b)-[e2:edge2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

--
-- Test 4: Two-hop patterns - backward direction
--

-- 4.1: Labeled end vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2]-(c:node3) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2]-(c:node3) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 4.2: Labeled start vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node3)<-[e1]-(b)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node3)<-[e1]-(b)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 4.3: Labeled middle vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b:node2)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b:node2)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 4.4: First edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1:edge2]-(b)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1:edge2]-(b)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 4.5: Last edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2:edge1]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2:edge1]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

--
-- Test 5: Three-hop patterns - forward direction
--

-- 5.1: Start vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e1]->(b)-[e2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e1]->(b)-[e2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 5.2: End vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d:node4) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d:node4) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 5.3: Labeled middle vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b:node2)-[e2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b:node2)-[e2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 5.4: First edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1:edge1]->(b)-[e2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1:edge1]->(b)-[e2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 5.5: Last edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b)-[e2]->(c)-[e3:edge3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b)-[e2]->(c)-[e3:edge3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 5.6 : Middle edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b)-[e2:edge2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b)-[e2:edge2]->(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

--
-- Test 6: Three-hop patterns - backward direction
--

-- 6.1: End vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d:node2) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d:node2) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 6.2: Start vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node4)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node4)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 6.3: Labeled middle vertex
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2]-(c:node3)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2]-(c:node3)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 6.4: First edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1:edge3]-(b)<-[e2]-(c)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1:edge3]-(b)<-[e2]-(c)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 6.5: Last edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2]-(c)<-[e3:edge1]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2]-(c)<-[e3:edge1]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

-- 6.6: Middle edge labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2:edge2]-(c)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2:edge2]-(c)<-[e3]-(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

--
-- Test 7: Four-hop patterns - forward direction (full chain)
--

-- 7.1: Start vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e1]->(b)-[e2]->(c)-[e3]->(d)-[e4]->(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e1]->(b)-[e2]->(c)-[e3]->(d)-[e4]->(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

-- 7.2: End vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d)-[e4]->(e:node5) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d)-[e4]->(e:node5) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

-- 7.3: Both ends labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e1]->(b)-[e2]->(c)-[e3]->(d)-[e4]->(e:node5) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e1]->(b)-[e2]->(c)-[e3]->(d)-[e4]->(e:node5) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

-- 7.4: Middle vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)-[e1]->(b)-[e2]->(c:node3)-[e3]->(d)-[e4]->(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)-[e1]->(b)-[e2]->(c:node3)-[e3]->(d)-[e4]->(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

--
-- Test 8: Four-hop patterns - backward direction (full chain)
--

-- 8.1: End vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d)<-[e4]-(e:node5) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d)<-[e4]-(e:node5) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

-- 8.2: Start vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node5)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d)<-[e4]-(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node5)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d)<-[e4]-(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

-- 8.3: Both ends labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node5)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d)<-[e4]-(e:node1) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node5)<-[e1]-(b)<-[e2]-(c)<-[e3]-(d)<-[e4]-(e:node1) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

-- 8.4: Middle vertex labeled
SELECT * FROM cypher('label_inference', $$
    MATCH (a)<-[e1]-(b)<-[e2]-(c:node3)<-[e3]-(d)<-[e4]-(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a)<-[e1]-(b)<-[e2]-(c:node3)<-[e3]-(d)<-[e4]-(e) RETURN a.id, b.id, c.id, d.id, e.id
$$) AS (a agtype, b agtype, c agtype, d agtype, e agtype);

--
-- Test 9: Mixed direction patterns
--

-- 9.1: Forward then backward
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e1]->(b)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e1]->(b)<-[e2]-(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 9.2: Backward then forward
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node3)<-[e1]-(b)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node3)<-[e1]-(b)-[e2]->(c) RETURN a.id, b.id, c.id
$$) AS (a agtype, b agtype, c agtype);

-- 9.3: Forward-backward-forward
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e1]->(b)<-[e2]-(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e1]->(b)<-[e2]-(c)-[e3]->(d) RETURN a.id, b.id, c.id, d.id
$$) AS (a agtype, b agtype, c agtype, d agtype);

--
-- Test 10: GUC disable test
--

-- 10.1: Disable inference and verify all labels are scanned
SET age.infer_labels = off;

SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

-- 10.2: Re-enable inference
SET age.infer_labels = on;

SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e]->(b) RETURN a.id, b.id
$$) AS (a agtype, b agtype);

--
-- Test 11: Multiple same-label edges
--

-- Add another edge with same labels
SELECT * FROM cypher('label_inference', $$
    CREATE (:node1 {id: 10})-[:edge1]->(:node2 {id: 20})
$$) AS (a agtype);

-- 11.1: Should return all matching edges
SELECT * FROM cypher('label_inference', $$
    MATCH (a:node1)-[e]->(b) RETURN a.id, b.id ORDER BY a.id
$$) AS (a agtype, b agtype);

SELECT * FROM cypher('label_inference', $$
    EXPLAIN (COSTS OFF) MATCH (a:node1)-[e]->(b) RETURN a.id, b.id ORDER BY a.id
$$) AS (a agtype, b agtype);

--
-- Cleanup
--
SELECT drop_graph('label_inference', true);
