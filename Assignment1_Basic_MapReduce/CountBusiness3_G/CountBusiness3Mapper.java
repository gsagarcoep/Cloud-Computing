import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CountBusiness3Mapper extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {
@Override
public void map(LongWritable key, MapWritable value,Context context)

			throws IOException, InterruptedException {

	Text city = new Text();
	Text businessId = new Text();

	city = (Text) value.get(new Text("city"));
	businessId = (Text) value.get(new Text("business_id"));
	
	//System.out.println("Key = "+key+", Value=  "+city);
	if(city.toString()!=null && !city.toString().isEmpty()){
		if(businessId.toString()!=null && !businessId.toString().isEmpty())
			context.write(city, new IntWritable(1));
	}
	
	}
}