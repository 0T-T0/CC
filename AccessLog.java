import java.io.IOException;
import java.util.Objects;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccessLog {
    /**
     *  Input:
     *      Object offset(LongWritable or Object), Text each line
     *  Output:
     *      Key, Value
     */
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String cur =  itr.nextToken().trim();
                // path match
                if(cur.equals("/assets/img/home-logo.png")){
                    word.set(cur);
                    context.write(word, one);
                }
                // ip addr match
                if(cur.equals("10.153.239.5")){
                    word.set(cur);
                    context.write(word, one);
                }

            }
        }
    }


    /**
     * INPUT FROM MAPPER:
     *      Text, IntWritable
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // configure for a job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "access log");
        // set .jar package path
        job.setJarByClass(AccessLog.class);

        // connect Mapper and Reducer
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // set Mapper key and value type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // set output key and value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // submit job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

