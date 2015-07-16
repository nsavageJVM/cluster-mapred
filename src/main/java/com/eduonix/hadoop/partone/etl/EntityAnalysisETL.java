package com.eduonix.hadoop.partone.etl;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.rockymadden.stringmetric.similarity.JaroMetric;
import com.rockymadden.stringmetric.similarity.OverlapMetric;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import scala.Option;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by ubu on 16.07.15.
 */
public class EntityAnalysisETL {


    private static final String END_CLUSTER_FLAG = "\tEND_CLUSTER_FLAG";
    private static final String START_CLUSTER_FLAG = "START_CLUSTER_FLAG";
    private static  boolean SET_CLUSTER_FLAG ;
    private static  boolean is_CLUSTER_LINE;
    private static final List<EntityStruct> entities = Lists.newLinkedList();
    private static final List<EntityStruct.DistanceStruct> clusters = Lists.newLinkedList();

    private static Path input;
    private static Path output;
    private static Pipeline pipeline;

    public static List<EntityStruct> extractData(Path dataIn) {

        // paths
        input = dataIn;
        pipeline = MemPipeline.getInstance();

        // crunch artifacts
        PCollection<String> lines = pipeline.read(From.textFile(input.toString()));
        PObject<Collection<String>> dirtyData = lines.asCollection();
        Iterator<String> tokenStore = dirtyData.getValue().iterator();

       // cluster artifacts
        String entityId = null;
        List<String> inputValues= null;

                // probably dont need here for clarity
        SET_CLUSTER_FLAG = false;
        is_CLUSTER_LINE= false;

        int token_counter = 0;

        while (tokenStore.hasNext()) {
            token_counter++;

            String[] line = null;
            String dirtyLine = tokenStore.next();

            if(dirtyLine.equals(START_CLUSTER_FLAG) ) {
                SET_CLUSTER_FLAG = true;
                is_CLUSTER_LINE = true;
                inputValues = Lists.newLinkedList();
            }


            if(dirtyLine.equals(END_CLUSTER_FLAG) ) {

                SET_CLUSTER_FLAG = false;
                is_CLUSTER_LINE = false;
            //    System.out.println("END_CLUSTER_FLAG size: " + inputValues.size()+ " clusterId: "+clusterId+ " for line: "+token_counter);
                EntityStruct cluster = new EntityStruct(entityId,  inputValues );

                entities.add(cluster);

            }

            if( SET_CLUSTER_FLAG  && is_CLUSTER_LINE ) {

                line = dirtyLine.toString().split("\t");
                entityId =  line[0];
          //      System.out.println("SET_CLUSTER_FLAG   && is_CLUSTER_LINE " + clusterId+ "for line "+token_counter);
                StringBuilder sb = new StringBuilder();

                for (int count = 1; count < line.length; count++ ) {
                    sb.append(line[count]);
                    if(count < line.length-1 && !line[count].trim().isEmpty() ) {
                        sb.append(",");
                    }
                }

                inputValues.add(sb.toString());
              //  System.out.println("SET_CLUSTER_FLAG  && is_CLUSTER_LINE inputValues" + inputValues.size()+ "for line "+token_counter);

            }






        }


        return entities;
    }

    public static List<EntityStruct.DistanceStruct> transformData( List<EntityStruct> entities) {


        for (EntityStruct entity : entities) {

            List<EntityStruct.DistanceStruct>  distances =   entity.getDistanceStructs();

            distances =  recurseEntity(distances);
            clusters.addAll(distances);

        }

        return clusters;
    }



    public static List<EntityStruct.DistanceStruct> recurseEntity(List<EntityStruct.DistanceStruct> distances ) {


        for (EntityStruct.DistanceStruct child : distances) {

            if(child.fileID.trim().isEmpty() ) continue;

            child.addressDistance = 0d;

            for (EntityStruct.DistanceStruct childLeaf : distances) {

              if(child.fileID.trim().equals(childLeaf.fileID.trim())) {
                  continue; }

              else {

                  if(childLeaf.fileID.trim().isEmpty() ) continue;
                  if(childLeaf.address.trim().isEmpty() ) continue;
                  if(child.address.trim().isEmpty() ) continue;

                  Double d = getOverLapDistance(child.address , childLeaf.address );

                  if(d == 1) {

                      child.duplicates.add(new  DuplicateStruct(childLeaf.entityId, childLeaf.fileID, childLeaf.address));

                  }



              }


            }


        }
        return distances;
    }


    static Double getOverLapDistance(String first_name , String second_name  ) {

        OverlapMetric metric = new OverlapMetric(1);
        Option<Object> result =  metric.compare(first_name, second_name);
        Double d = (Double) result.toList().head();
   //     String metricValuesDebug =  String.format("OverlapMetric for %s   computes metric value : %f", first_name+" : "+second_name, d );
 //       System.out.println(metricValuesDebug);

        return d;
    }


    public static void loadData(List<String> duplicates, Path outputFile) {

        PCollection<String> lines = MemPipeline.collectionOf(duplicates);

        pipeline.write(lines, To.textFile(outputFile.toString()));



    }
}
