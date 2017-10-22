package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PointsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private String numPart;
	
	@Override
	protected void setup(Mapper.Context context) throws IOException,InterruptedException{
		
		numPart=context.getConfiguration().get("numPart");
		
	}
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		int number_part=Integer.parseInt(numPart);
		context.write(new LongWritable(key.get()%number_part), value);
	}
	

}
