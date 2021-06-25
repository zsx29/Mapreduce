package sub1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// map 연산을 수행하는 클래스
public class KeywordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private IntWritable one = new IntWritable(1);
	private Text txt = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] tokens = line.split(",");
		
		txt.set(tokens[1]);
		context.write(txt, one);
	}
}










