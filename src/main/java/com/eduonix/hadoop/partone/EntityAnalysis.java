package com.eduonix.hadoop.partone;

import java.io.* ;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration ;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by ubu on 11.07.15.
 */
public class EntityAnalysis extends Configured implements Tool {

    private static final String projectRootPath = System.getProperty("user.dir");

    private static final String raw_data = "ComercialBanks.csv";
    private static final String mapped_data = "output"+System.currentTimeMillis();;

    private static Path outputFile;
    private static Path inputFile;


    public static void main(String[] args) throws Exception
    {
        // this main function will call run method defined above.
        int res = ToolRunner.run(new Configuration(), new EntityAnalysis(), args);
        System.exit(res);
    }



    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        outputFile = new Path(projectRootPath, mapped_data);
        inputFile = new Path(projectRootPath, raw_data);
        Job job = Job.getInstance(conf);

        job.setJarByClass(EntityAnalysis.class);
        job.setMapperClass(EntityMapper.class);
        job.setReducerClass(EntityReducer.class);

        // these values define the types for the MAGIC shuffle sort steps
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        return (job.waitForCompletion(true) ? 0 : 1);
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


}
