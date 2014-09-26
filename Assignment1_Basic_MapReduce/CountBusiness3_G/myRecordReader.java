import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class myRecordReader 
	extends RecordReader<LongWritable, MapWritable>{

	 private static final Logger LOG =
			 LoggerFactory.getLogger(myRecordReader
			 .class);
	 
	 private LineRecordReader reader = new LineRecordReader();
	 private final Text currentLine_ = new Text();
	 private final MapWritable mapValue_ = new MapWritable();
	 private final JSONParser jsonParser_ = new JSONParser();
 
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		reader.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return reader.getCurrentKey();
	}

	@Override
	public MapWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return mapValue_;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		reader.initialize(arg0,arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		while (reader.nextKeyValue()) {
			mapValue_.clear();
			if (parseLineJson(jsonParser_, reader.getCurrentValue(),mapValue_)) {
				return true;
			}
		}
		return false;
	}
	
	/********************* JSON Parsing ******************************************/
	public static boolean parseLineJson(JSONParser parser,
			Text line,
			MapWritable value){
		Log.info("Parsing string = "+line);
		try{
		
			JSONObject jsonObj = (JSONObject) parser.parse(line.toString());
			String cityName= jsonObj.get("city").toString();
			Log.info("Adding city = "+cityName);
			value.put(new Text("city"),new Text(cityName));
			
			String busiId= jsonObj.get("business_id").toString();
			Log.info("Adding city = "+cityName);
			value.put(new Text("business_id"),new Text(busiId));
			
			return true;
		
		}catch (ParseException e) {
	
			LOG.warn("Could not json-decode string: " + line, e);
			return false;
		
		} catch (NumberFormatException e) {
		
			LOG.warn("Could not parse field into number: " + line, e);
			return false;
		
		}
	}
	
}
