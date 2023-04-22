package lab2.InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    private Text key_info = new Text(); // 将单词和所在文件名结合作为key "a:file1"
    private Text value_info = new Text("1"); // 词频作为value
    private FileSplit split;
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        split = (FileSplit)context.getInputSplit();
        StringTokenizer st = new StringTokenizer(value.toString());
        while (st.hasMoreTokens()) {
            key_info.set(st.nextToken() + ":" + split.getPath().toString().substring(split.getPath().toString().lastIndexOf("/") + 1));
            context.write(key_info, value_info);
        }
    }
}
