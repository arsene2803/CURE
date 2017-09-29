package Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.api.records.Priority;

import util.Cluster;
import util.Point; 

public class Curereducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key,Iterable<Text> values,OutputCollector<LongWritable, Text> output) throws IOException {
		
		//each point will be individual cluster
		List<Cluster> cl=new ArrayList<>();
		for(Text val:values) {
			int i=0;
			double[] coord=new double[2]; 
			String line=val.toString();
			StringTokenizer st=new StringTokenizer(line);
			while(st.hasMoreTokens()) {
				coord[i++]=Double.parseDouble(st.nextToken());
			}
			//create individual clusters
			List<Point> temp=new ArrayList<>();
			temp.add(new Point(coord[0], coord[1]));
			cl.add(new Cluster(null,temp,0));
		}
		
		//compute the closest cluster for indivdual clusters
		for(int i=0;i<cl.size();i++) {
			double min_distance=Double.MAX_VALUE;
			int min_cluster_index=i;
			double x1=cl.get(i).getRep().get(0).getX();
			double y1=cl.get(i).getRep().get(0).getY();
			for(int j=0;j<cl.size();j++) {
				if(i!=j) {
					double x2=cl.get(j).getRep().get(0).getX();
					double y2=cl.get(j).getRep().get(0).getY();
					double distance=Math.sqrt(Math.pow(Math.abs(x1-x2), 2)+Math.pow(Math.abs(y1-y2), 2));
					if(distance<min_distance) {
						min_distance=distance;
						min_cluster_index=j;
					}
					
				}
			}
			//setting the closest and min_distance
			cl.get(i).setClosest(cl.get(min_cluster_index));
			cl.get(i).setMin_distance(min_distance);
			
		}
		
		//setting the heap
		PriorityQueue<Cluster> T=new PriorityQueue<>(cl);
		
		
		
		
		
		
	}
	

}
