import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountBusiness3Reducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

@Override
public void reduce(Text key, Iterable<IntWritable> values,
    Context context)
    throws IOException, InterruptedException {

   int maxValue = Integer.MIN_VALUE;
  int count=0;
  String prev=null;
  for (IntWritable value : values) {
	  maxValue = Math.max(maxValue, value.get());
	  if(prev !=null && key.toString().compareTo(prev)==0){
		  count+=maxValue;
		  continue;
	  }
	  else if(prev!=null){
		  context.write(new Text(prev), new IntWritable(count));
		  count=0;
		  prev=key.toString();
		  continue;
	  }
	  prev=key.toString();
	  count++;
  }
  if(prev!=null){
	  context.write(new Text(prev), new IntWritable(count));
	
  }
 }
}