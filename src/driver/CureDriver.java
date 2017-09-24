package driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class CureDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//creating configuration object
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"CUREJOB");
		job.setJarByClass(CureDriver.class);
	    job.setMapperClass(mapper.PointsMapper.class);
	    job.setCombinerClass(Reducer.Curereducer.class);
	    job.setReducerClass(Reducer.Curereducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    //setting the format
	    job.setInputFormatClass(util.SamplingInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}

}
