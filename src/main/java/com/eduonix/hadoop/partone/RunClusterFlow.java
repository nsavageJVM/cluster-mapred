package com.eduonix.hadoop.partone;

import com.eduonix.hadoop.partone.etl.DuplicateStruct;
import com.eduonix.hadoop.partone.etl.EntityAnalysisETL;
import com.eduonix.hadoop.partone.etl.EntityStruct;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

/**
 * Created by ubu on 16.07.15.
 */
public class RunClusterFlow {

    private static final String projectRootPath = System.getProperty("user.dir");
    private static final String mapped_data = "output";

    public static void main(String[] args) {

        Path inputFile = Paths.get(projectRootPath, mapped_data);

        List<EntityStruct> entities = EntityAnalysisETL.loadData(inputFile);


        List<EntityStruct.DistanceStruct> clusters =  EntityAnalysisETL.transformData(entities);


        for (EntityStruct.DistanceStruct cluster : clusters) {

            if(cluster.duplicates.size() > 0 ) {


                for (DuplicateStruct duplicate : cluster.duplicates) {

                    String[] addressLine = duplicate.value.split(" ");

                    if (addressLine.length > 1 ) {
                        System.out.println(duplicate);
                    }


                }

            }


        }
    }
}
