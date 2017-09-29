package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PointsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	LongWritable newKey=new LongWritable(1);
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		context.write(newKey, value);
	}
	

}
