import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This class is responsible for running map reduce job*/
public class HelloDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception
	{
	
		if(args.length !=2) {
			System.err.println("Usage: InvertedIndex_G <input path> <outputpath>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(HelloDriver.class);
		job.setJobName("Inverted Index");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(ReviewByMonth4Mapper.class);
		job.setReducerClass(ReviewByMonth4Reducer.class);
		job.setInputFormatClass(myInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1); 
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		HelloDriver driver = new HelloDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}