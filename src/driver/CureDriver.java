package driver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import partitioning.keyPartitioner;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;


public class CureDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//Checking for the number of arguments
		if(args.length != 5) {
			System.out.println(args.length);
			throw new IllegalArgumentException("Arguments expected- input output Number_Of_partitions first_pass_dir first_pass_file_name"
					);
		}
		
		//creating configuration object
		Configuration conf=new Configuration();
		conf.set("numPart",args[2]);
		Job job=Job.getInstance(conf,"FIRST_PASS_CUREJOB");
		job.setJarByClass(CureDriver.class);
	    job.setMapperClass(mapper.PointsMapper.class);
	    job.setReducerClass(Reducer.Curereducer.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setPartitionerClass(keyPartitioner.class);
	    job.setNumReduceTasks(Integer.parseInt(args[2]));
	    //setting the format
	    job.setInputFormatClass(util.SamplingInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    /*if(!job.waitForCompletion(true)) {
	    	aggregateFirstPass(args, conf);
	    }
	    //create a new job
	    conf=new Configuration();
	    job=Job.getInstance(conf,"SECOND_PASS_CUREJOB");*/
	    
	}

	public static void aggregateFirstPass(String[] args, Configuration conf) throws IOException, FileNotFoundException {
		//aggregating all the clusters
	    FileSystem fs=FileSystem.get(conf);
	    FileStatus[] fss = fs.listStatus(new Path("/"));
	    //making final output mkdir
	    Path final_dir=new Path("/"+args[3]);
	    fs.mkdirs(final_dir);
	    Path first_pass_res=new Path(final_dir+"/"+args[4]);
	    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(first_pass_res), SequenceFile.Writer.keyClass(Text.class),SequenceFile.Writer.valueClass(Text.class));
	    for (FileStatus status : fss) {
	        Path path = status.getPath();
	        SequenceFile.Reader reader = new SequenceFile.Reader(conf,Reader.file(path));
	        Text key = new Text();
	        Text value = new Text();
	        while (reader.next(key, value)) {
	            writer.append(key.toString(), value.toString());
	        }	
	    }
	    writer.close();
	}
	
}
