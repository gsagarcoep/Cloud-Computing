import java.io.IOException;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ReviewByMonth4Mapper extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {
@Override
public void map(LongWritable key, MapWritable value,Context context)

			throws IOException, InterruptedException {

	Text user = new Text();
	Text review = new Text();
	Text date = new Text();

	user = (Text) value.get(new Text("user_id"));
	review = (Text) value.get(new Text("review_id"));
	date = (Text) value.get(new Text("date"));
	
	System.out.println("-------------------------------------------------  "+date.toString());
	if(user.toString()!=null && !user.toString().isEmpty()){
		if(review.toString()!=null && !review.toString().isEmpty())
			if(date.toString()!=null && !date.toString().isEmpty()){
				
				String month_name ="";
				String season ="";
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				DateFormatSymbols dfs = new DateFormatSymbols();
				String[] months = dfs.getMonths();
				 
				try {
					Date d1 = df.parse(date.toString());
					int month_id= d1.getMonth();
					Calendar cal = new GregorianCalendar(d1.getYear()+1900,month_id,d1.getDay());
					
					if(month_id==10 || month_id==11 || month_id==0 || month_id==1)
						season="winter";
					else if(month_id==2 || month_id==3 || month_id==4 || month_id==5)
						season="spring";
					else if(month_id==6 || month_id==7 || month_id==8 || month_id==9)
						season="monsoon";
					
					month_name=months[month_id].toLowerCase();
					
			//		System.out.println(" "+month_id+" "+month_name+" "+season);
					
					context.write(new Text(month_name), new IntWritable(1));
					context.write(new Text(season), new IntWritable(1));
					
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				
			}
	}
	
	}
}