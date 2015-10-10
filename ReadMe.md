## Clustering Example

- In many clustering applications the first step is to find the equivalent entities and build the clusters from these   
  hear we can exploit the fact that map-reduce aggregates the identical keys, by selecting the key as the match entity  
  we can create initial seed clusters, then we an apply a metric to aggregate the remaining data to these centers  
  
  
  
    100% equivalent              node1             node2 .....  
    90%  equivalent   subnode 1a      subnode 1a ............                 
    repeat
    
    
- To run code
 * 1 run com.eduonix.hadoop.partone.EntityAnalysisMRJob to produce the traning data, set    
   `public static final  boolean runOnCluster = false` for local testing on Linux  
    `public static final  boolean runOnCluster = true` for Hadoop  
 * 2 run  com.eduonix.hadoop.partone.etl.EntityAnalysisETL  
    
   