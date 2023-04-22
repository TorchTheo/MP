package lab2.InvertedIndex;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String file_list = new String();
        for (Text value : values) {
            file_list += value.toString() + "; ";
        }
        StringTokenizer st = new StringTokenizer(file_list.toString());
        float books = 0;
        float sum = 0;
        while (st.hasMoreTokens()) {
            books += 1;
            String com = st.nextToken();
            int index1 = com.indexOf(':');
            int index2 = com.lastIndexOf(';');
            sum += Integer.parseInt(com.substring(index1 + 1, index2));
        }
        // 保留两位小数
        DecimalFormat df = new DecimalFormat(".00");
        String avg = df.format(sum / books) + ", ";
        // 格式化输出
        key.set("[" + key.toString() + "]");
        result.set(avg + file_list);
        context.write(key, result);
    }
}
