package Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

public class Curereducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key,Iterable<Text> values,OutputCollector<LongWritable, Text> output) throws IOException {
		for(Text val:values) {
			output.collect(key, val);
		}
		
		
	}
	

}
