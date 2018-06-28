import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by TeamZero on 31/01/17.
 */
public class Common {
    /**
     * this class is responsible for creating and runing a hadoop MapReduce job
     * @param conf job configuration
     * @param job_name job name
     * @param className define jar by class name
     * @param mapper mapper class
     * @param reducer reducer class
     * @param combiner combiner class. null if you don't have any combiner.
     * @param mapOutputKey mapper output key type
     * @param mapOutputValue mapper output value type
     * @param outputKey reducer output key type
     * @param outputValue reducer output value type
     * @param input job input
     * @param output job output
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void jobRunner(Configuration conf, String job_name, Class<?> className,
                                 Class<? extends Mapper> mapper, Class<? extends Reducer> reducer,
                                 Class<? extends Reducer> combiner, Class<?> mapOutputKey, Class<?> mapOutputValue,
                                 Class<?> outputKey, Class<?> outputValue, Path input, Path output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, job_name);
        job.setJarByClass(className);
        job.setMapperClass(mapper);
        // in a case that we have a combiner
        if(combiner != null){
            job.setCombinerClass(combiner);
        }
        // in a case that we have a reducer
        if(reducer != null){
            job.setReducerClass(reducer);
            //reducer output key type
            job.setOutputKeyClass(outputKey);
            //reducer output value type
            job.setOutputValueClass(outputValue);
        }
        //mapper output key type
        job.setMapOutputKeyClass(mapOutputKey);
        //mapper output value type
        job.setMapOutputValueClass(mapOutputValue);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);
    }
}
