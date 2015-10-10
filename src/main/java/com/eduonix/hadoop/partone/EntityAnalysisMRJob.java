package com.eduonix.hadoop.partone;

import java.io.* ;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by ubu on 11.07.15.
 */
public class EntityAnalysisMRJob extends Configured implements Tool {

    private static final String projectRootPath = System.getProperty("user.dir");
    public static final  boolean runOnCluster = false;

    private static final String END_CLUSTER_FLAG = "END_CLUSTER_FLAG";
    private static final String START_CLUSTER_FLAG = "START_CLUSTER_FLAG";

    private static final String raw_data = "ComercialBanks10k.csv";
    private static final String mapped_data = "output";
    private static final String mappedDataForAnalysis = "/mapped_data";

    private static Path outputFile;
    private static Path inputFile;
    private static Path mappedDataPath;
    private static Configuration conf;

    public static void main(String[] args) throws Exception
    {
        // this main function will call run method defined above.
        int res = ToolRunner.run(new Configuration(), new EntityAnalysisMRJob(), args);
        System.out.println("res: "+res);
        if(res==0  && runOnCluster) {
            runMigrate();
        }

        System.exit(res);
    }



    public int run(String[] strings) throws Exception {
        conf = getConf();

        File localOutputDirectory = new File(String.format("%s%s",projectRootPath ,"/output" ));

        if(localOutputDirectory.exists()) {

            System.out.println("Mapreduce output folder exists in local filesystem  deleting run local test again");
            delete(localOutputDirectory);

            return 1;

        }

        File localMappedDirectory = new File(String.format("%s%s",projectRootPath ,"/mapped_data" ));

        if(localMappedDirectory.exists()) {

            System.out.println("Mapped data output folder exists in local filesystem  deleting run local test again");
            delete(localMappedDirectory);

            return 1;

        }


        outputFile = new Path(projectRootPath, mapped_data);
        inputFile = new Path(projectRootPath, raw_data);
        Job job = Job.getInstance(conf);

        job.setJarByClass(EntityAnalysisMRJob.class);
        job.setMapperClass(EntityMapper.class);
        job.setReducerClass(EntityReducerClusterSeeds.class);

        // these values define the types for the MAGIC shuffle sort steps
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static int runMigrate( ) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path( projectRootPath);
        mappedDataPath = new Path( tmpPath.toString()+mappedDataForAnalysis);

        System.out.println( String.format("  mappedDataPath %s", mappedDataPath ));

        fs.copyToLocalFile(false, outputFile, mappedDataPath);
        fs.copyToLocalFile(false, inputFile, mappedDataPath);

        return 0;
    }




    public static class EntityMapper  extends Mapper<Object, Text, Text, Text>
    {
        private final static Text valueLine = new Text();
        private Text keyWord = new Text();
        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {

            String[] line = value.toString().split("\\t") ;
            if(line[0].equals("ID")) {
              return;
            }

            // set the name of the bank as our key
            keyWord.set(line[1]);
            valueLine.set(value);
            // emitt
            context.write(keyWord, valueLine);
        }
    }

    public static class EntityReducer  extends Reducer<Text,Text,Text,IntWritable> {

        public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

            int total = 0;

            for (Text val : values) {
                total++ ;
            }
            if( total > 1)  {
                // found a possible duplicate
                StringBuilder logger = new StringBuilder();
                logger.append("Found Duplicate Values for ").append(key).append("\n");

                for (Text val : values) {
                    logger.append(val).append("\n");
                }

                context.write( new Text(logger.toString()), new IntWritable(total));

            } else {
                context.write(key, new IntWritable(total));
            }


        }
    }


    public static class EntityReducerClusterSeeds  extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

            int total = 0;
            StringBuilder logger = new StringBuilder();
            logger.append(START_CLUSTER_FLAG).append("\n");
            for (Text val : values) {
                total++ ;
                logger.append(key).append("\t").append(val).append("\n");
            }
            if( total > 1)  {

                context.write(new Text(logger.toString()), new Text(END_CLUSTER_FLAG));

            }


        }
    }



    public static void delete(File file)
            throws IOException{

        if(file.isDirectory()){

            //directory is empty, then delete it
            if(file.list().length==0){

                file.delete();
                System.out.println("Directory is deleted : "
                        + file.getAbsolutePath());

            }else{

                //list all the directory contents
                String files[] = file.list();

                for (String temp : files) {
                    //construct the file structure
                    File fileDelete = new File(file, temp);

                    //recursive delete
                    delete(fileDelete);
                }

                //check the directory again, if empty then delete it
                if(file.list().length==0){
                    file.delete();
                    System.out.println("Directory is deleted : "
                            + file.getAbsolutePath());
                }
            }

        }else{
            //if file, then delete it
            file.delete();
            System.out.println("File is deleted : " + file.getAbsolutePath());
        }
    }
}


