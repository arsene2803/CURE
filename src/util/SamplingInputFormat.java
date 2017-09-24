package util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;


public class SamplingInputFormat extends TextInputFormat{

	@Override
	public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext arg0) throws IOException {
		// TODO Auto-generated method stub
		int i;
		Random r=new Random();
		final double samp_percen=0.7;
		List<InputSplit> totalIs=super.getSplits(arg0);
		int k=(int) (totalIs.size() *samp_percen);
		
		if(k==0) {
			return totalIs;
		}
		
		List<InputSplit> sampIs=new ArrayList();
		for(i=0;i<k;i++)
			sampIs.add(totalIs.get(i));
		
		for(;i<totalIs.size();i++) {
			int j=r.nextInt(i+1);
			if(j<k)
				sampIs.set(i, totalIs.get(i));
		}
		return sampIs;
	}

}
