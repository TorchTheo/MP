package lab2.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    private Text new_value = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text value : values) {
            sum += Integer.parseInt(value.toString());
        }
        int index = key.toString().indexOf(':'); // 找到':'所在位置进行分割
        new_value.set(key.toString().substring(index + 1) + ":" + sum);
        key.set(key.toString().substring(0, index));
        context.write(key, new_value);
    }
}
