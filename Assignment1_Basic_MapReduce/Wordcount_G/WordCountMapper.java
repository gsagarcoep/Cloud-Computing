import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().toLowerCase();
		//String year = line.substring(15, 19);
	
		StringTokenizer st = new StringTokenizer(line, " ");
		while(st.hasMoreTokens()){
			String word = st.nextToken();
	//		System.out.println(word+" + 1 ");
			context.write(new Text(word), new IntWritable(1));
		}
	}
}