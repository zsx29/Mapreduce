package sub1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reduce 연산을 수행하는 클래스
public class KeywordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	private Map<String, Integer> map = new HashMap<>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		for(IntWritable val : values) {
			sum += val.get();
		}
		
		map.put(key.toString(), sum);
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// 최종 마지막 리듀스 작업 후 실해되는 메서드
		List<String> list = new ArrayList<>();
		list.addAll(map.keySet());
	
		Collections.sort(list, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				
				Integer v1 = map.get(o1);
				Integer v2 = map.get(o2);
				
				int result = v1.compareTo(v2);
				return result;
			}
		});
		
		Collections.reverse(list);
		
		Iterator<String> iter = list.iterator();
		
		while(iter.hasNext()) {
			
			String word =  iter.next();
			int sum = map.get(word);
			
			context.write(new Text(word), new IntWritable(sum));
		}
	}
}












