import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

@Override
public void reduce(Text key, Iterable<IntWritable> values,
    Context context)
    throws IOException, InterruptedException {
	//System.out.println("********************************************************************************************");
  int maxValue = Integer.MIN_VALUE;
  int count=1;
  String prev=null;
  for (IntWritable value : values) {
	  maxValue = Math.max(maxValue, value.get());
	  //System.out.println("key = "+key+"  --Value = "+value.get());
	  if(prev !=null && key.toString().compareTo(prev)==0){
		  count+=maxValue;
		  continue;
	  }
	  else if(prev!=null){
		  context.write(new Text(prev), new IntWritable(count));
		//  System.out.println("key = "+prev+"  --Value = "+count);
		  prev=key.toString();
		  continue;
	  }
	  prev=key.toString();
  
  }
  if(prev!=null){
	  context.write(new Text(prev), new IntWritable(count));
//	  System.out.println("last key == "+prev+"  --Value = "+count);
	
  }
  System.out.println("********************************************************************************************");
}
}