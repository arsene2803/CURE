package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ClusterMapper extends Mapper<Text, Text, LongWritable, Text> {
	public void map(Text key, Text value,Context context) throws IOException, InterruptedException {
		context.write(new LongWritable(1), new Text(key.toString()+"|"+value.toString()));
	}
}
