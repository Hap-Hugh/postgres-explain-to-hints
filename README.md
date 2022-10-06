# postgres-explain-decoder

Input: postgres explain (json format)
Output: 
Join order;   e.g. ( ( ( (  mc  mi_idx )  t )  it )  ct)
Join method on each join condition;   MergeJoin( mc  mi_idx)
Scan method on each table;  IndexScan on Table’s alias
