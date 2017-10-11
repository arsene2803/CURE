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

import kd_tree.kdtree;
import util.Cluster;
import util.Point; 

public class Curereducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key,Iterable<Text> values,OutputCollector<LongWritable, Text> output) throws IOException {
		//need to set the number of clusters k,the shrinking factor alpha and the number of scattered points
		int k=5,c=56;
		double alpha=0.8;
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
		
		//compute the closest cluster for individual clusters
		for(int i=0;i<cl.size();i++) {
			double min_distance=Double.MAX_VALUE;
			int min_cluster_index=i;
			double x1=cl.get(i).getRep().get(0).getX();
			double y1=cl.get(i).getRep().get(0).getY();
			for(int j=0;j<cl.size();j++) {
				if(i!=j && cl.get(i).getClosest()==null) {
					double x2=cl.get(j).getRep().get(0).getX();
					double y2=cl.get(j).getRep().get(0).getY();
					double distance=Math.sqrt(Math.pow(Math.abs(x1-x2), 2)+Math.pow(Math.abs(y1-y2), 2));
					if(distance<min_distance) {
						min_distance=distance;
						min_cluster_index=j;
					}
					
				}
			}
			//setting the closest and min_distance and the mean
			cl.get(i).setClosest(cl.get(min_cluster_index));
			cl.get(i).setMin_distance(min_distance);
			cl.get(i).setMean();
			cl.get(min_cluster_index).setClosest(cl.get(i));
			cl.get(min_cluster_index).setMin_distance(min_distance);
			cl.get(min_cluster_index).setMean();
			
		}
		
		//setting the heap
		PriorityQueue<Cluster> Q=new PriorityQueue<>(cl);
		//getting the list of points
		List<Point> pl=new ArrayList<>();
		for(int i=0;i<cl.size();i++) {
			setClusterPoint(cl.get(i));
			pl.addAll(cl.get(i).getRep());
		}
		//setting the kd tree
		kdtree T=new kdtree(pl);
		while(Q.size()<k) {
			Cluster u=Q.poll();
			Cluster v=u.getClosest();
			//delete v from priority queue
			Q.remove(v);
			Cluster w=merge(u,v,c,alpha);
		}
		
		
		
		
		
		
	}
	
	private Cluster merge(Cluster u, Cluster v,int c,double alpha) {
		// TODO Auto-generated method stub
		List<Point> pl=new ArrayList();
		List<Point> temp=new ArrayList();
		pl.addAll(u.getRep());
		pl.addAll(v.getRep());
		Cluster w=new Cluster(null, pl, 0);
		w.setMean();
		//getting the scattered points
		if(pl.size()>c) {

			for(int i=0;i<c;i++) {
				double maxDist=0;
				Point maxPoint=null;
				for(int j=0;j<pl.size();j++) {
					double minDist=0;
					if(i==0) {
						minDist=getdist(w.getMean(),pl.get(j),alpha);
					}
					else {
						minDist=Double.MAX_VALUE;
						for(int k=0;k<temp.size();k++) {
							double dist=getdist(temp.get(k),pl.get(j),alpha);
							if(dist<minDist)
								minDist=dist;
						}
					}
					if(minDist>=maxDist) {
						maxDist=minDist;
						maxPoint=pl.get(j);
					}
					
				}
				temp.add(maxPoint);
			}	
		}
		else {
			temp=pl;
		}
		
		for(int i=0;i<temp.size();i++) {
			Point p=temp.get(i);
			p.setX(p.getX()+alpha*(w.getMean().getX()-p.getX()));
			p.setY(p.getY()+alpha*(w.getMean().getY()-p.getY()));
		}
		w.setMean();
		return w;
		
		
	}

	private double getdist(Point mean, Point point,double alpha) {
		// TODO Auto-generated method stub
		double sx=point.getX()-alpha*(point.getC().getMean().getX()-point.getX());
		double sy=point.getY()-alpha*(point.getC().getMean().getY()-point.getY());
		double dx=mean.getX()-sx;
		double dy=mean.getY()-sy;
		return Math.sqrt(dx*dx+dy*dy);
	}

	public void setClusterPoint(Cluster c) {
		if(c.getRep()!=null) {
			List<Point> pl=c.getRep();
			for(int i=0;i<pl.size();i++) {
				pl.get(i).setC(c);
			}
		}
		
	}
	

}
