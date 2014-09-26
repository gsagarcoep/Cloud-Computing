import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class myInputFormat 
	extends	FileInputFormat<LongWritable, MapWritable>{

	private static final Logger log =
			LoggerFactory.getLogger(myInputFormat.class);
	@Override
	public RecordReader<LongWritable, MapWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new myRecordReader();
	}
	

}
