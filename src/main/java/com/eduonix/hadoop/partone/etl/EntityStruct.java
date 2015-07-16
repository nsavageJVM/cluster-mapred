package com.eduonix.hadoop.partone.etl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by ubu on 16.07.15.
 */
public class EntityStruct {

    public String entityId;


    private List<DistanceStruct> distances;
    private List<DuplicateStruct> duplicates;


    public  List<String>  values;


    public class DistanceStruct {
        public List<DuplicateStruct> duplicates = Lists.newLinkedList();
        public String entityId;
        public Double addressDistance;
        public String address;
        public String fileID;

    }



    public EntityStruct( String entityId, List<String> inputValues ) {
        distances = Lists.newLinkedList();

        Iterator<String> itr = inputValues.iterator();

        values = Lists.newLinkedList();

        this.entityId =entityId;

        while (itr.hasNext()) {
            String next = itr.next();
            values.add(next);
        }

        }

    public List<DistanceStruct> getDistanceStructs () {

        for (String value : values) {
            String[] line = value.toString().split(",");
            DistanceStruct clusterBag = new DistanceStruct();
            clusterBag.entityId = entityId;
            clusterBag.fileID = line[0];
            StringBuffer sb = new StringBuffer();
            for (int i=2; i < line.length; i++) {
                if (i != 2) sb.append(" ");
                sb.append(line[i]);
            }
            clusterBag.address = sb.toString();

            distances.add(clusterBag);
        }
        return distances;
    }







    }

