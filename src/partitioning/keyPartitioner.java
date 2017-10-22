package partitioning;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class keyPartitioner  extends Partitioner<LongWritable,Text>{

	@Override
	public int getPartition(LongWritable key, Text value, int numReduceTask) {
		// TODO Auto-generated method stub
		return (int) (key.get()%numReduceTask);
	}

}
