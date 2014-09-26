import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer
extends Reducer<Text, Text, Text, Text> {

@Override
//public void reduce(Text key, Iterable<Text> values,	OutputCollector<Text, Text> output,Reporter reporter)
//    throws IOException, InterruptedException {
public void reduce(Text key, Iterable<Text> values,	Context output)
	    throws IOException, InterruptedException {	
 
	// System.out.println("********************************************************************************************");
  String maxValue = null;
  String count="";
  String prev=null;
  for (Text value : values) {
	  maxValue = value.toString();
	  //System.out.println("key = "+key+"  --Value = "+value.get());
	  if(prev !=null && key.toString().compareTo(prev)==0){
		  count+=","+maxValue;
		  continue;
	  }
	  else if(prev!=null){
		output.write(new Text(prev), new Text(count));
		//System.out.println("key = "+prev+"  --Value = "+count);
		count="";
		prev=key.toString();
		  continue;
	  }
	  prev=key.toString();
	  count=maxValue;
  }
  if(prev!=null){
	  output.write(new Text(prev), new Text(count));
	  //System.out.println("last key == "+prev+"  --Value = "+count);
	  count="";
  }
  //System.out.println("********************************************************************************************");
}
}