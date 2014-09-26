import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends
		Mapper<LongWritable, Text, Text, Text> {
@Override
public void map(LongWritable key, Text value,Context output)

			throws IOException, InterruptedException {

		String line = value.toString().toLowerCase();
		//String year = line.substring(15, 19);
	//	System.out.println("------------------------------------------------");
		StringTokenizer st = new StringTokenizer(line, ",");
		String site = null;
		while(st.hasMoreTokens()){
			String word = st.nextToken();
			if(site==null){
				site=word;
		//		System.out.println(word+" + site ");
				continue;
			}
			else//System.out.println(word+" + words ");
				output.write(new Text(word), new Text(site));
	//		System.out.println(word+" "+ site );
			
		}
	}
}