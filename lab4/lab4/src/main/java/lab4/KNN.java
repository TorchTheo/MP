package lab4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KNN {


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.addCacheFile(new URI("/data/exp4/iris_test.csv"));
        job.setJarByClass(KNN.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(KNNMapper.class);
        job.setReducerClass(KNNReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class KNNMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        Vector<String> vec = new Vector<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
            FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = bufferedReader.readLine()) != null){
                vec.add(line);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] train_data_strs = value.toString().split(",");
            int num = 0;
            for (String test_data : vec) {
                String[] test_data_strs = test_data.split(",");
                double distance = 0;
                for (int i = 0; i < 4; i++)
                    distance += Math.pow(Double.parseDouble(test_data_strs[i]) - Double.parseDouble(train_data_strs[i]), 2);
                context.write(new LongWritable(num), new Text(distance + "," + train_data_strs[4]));
                num++;
            }
        }
    }

    public static class KNNReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Double, String> d_s_map = new TreeMap<>();
            for (Text value : values) {
                String[] split = value.toString().split(",");
                d_s_map.put(Double.parseDouble(split[0]), split[1]);
            }
            HashMap<String, Integer> s_n_map = new HashMap<>();
            int K = 5;
            for (Double dis : d_s_map.keySet()) {
                if (K == 0)
                    break;
                String sig = d_s_map.get(dis);
                if (s_n_map.containsKey(sig))
                    s_n_map.put(sig, s_n_map.get(sig) + 1);
                else
                    s_n_map.put(sig, 1);
                K--;
            }
            int max_count = Integer.MIN_VALUE;
            String ret_sig = null;
            for (String sig : s_n_map.keySet()) {
                if (max_count < s_n_map.get(sig)) {
                    max_count = s_n_map.get(sig);
                    ret_sig = sig;
                }
            }
            context.write(key, new Text(ret_sig));
        }
    }
}
