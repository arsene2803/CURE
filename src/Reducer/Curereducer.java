package Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

public class Curereducer extends Reducer<LongWritable, Text, Text, Text> {
	private String pass;
	
	@Override
	protected void setup(Reducer<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		pass=context.getConfiguration().get("pass");
	}
	public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		//need to set the number of clusters k,the shrinking factor alpha and the number of scattered points
		int c=56,k;
		double alpha=0.8;
		//each point will be individual cluster
		List<Cluster> cl=new ArrayList<>();
		if(pass.equals("1")) {
			k=300;
			for(Text val:values) {
				int i=0;
				double[] coord=new double[2]; 
				String line=val.toString();
				StringTokenizer st=new StringTokenizer(line,",");
				while(st.hasMoreTokens()) {
					coord[i++]=Double.parseDouble(st.nextToken().replace("\"", ""));
				}
				//create individual clusters
				List<Point> temp=new ArrayList<>();
				temp.add(new Point(coord[0], coord[1]));
				cl.add(new Cluster(null,temp,0));
			}
		}else {
			k=10;
			Map<String,List<Point>> hmap=new HashMap<>();
			for(Text val: values) {
				int i=0;
				StringTokenizer st=new StringTokenizer(val.toString(),"|");
				String keyValue=st.nextToken().trim();
				String value=st.nextToken();
				st=new StringTokenizer(value,",");
				double[] coord=new double[2];
				while(st.hasMoreTokens()) {
					coord[i++]=Double.parseDouble(st.nextToken().replace("\"", "").trim());
				}
				if(hmap.containsKey(keyValue)) {
					hmap.get(keyValue).add(new Point(coord[0], coord[1]));
				}
				else {
					List<Point> temp=new ArrayList<>();
					temp.add(new Point(coord[0], coord[1]));
					hmap.put(keyValue, temp);
				}
			}
			//iterate through hash map
			for(List<Point> plist:hmap.values()) {
				cl.add(new Cluster(null,plist,0));
			}
		}
		
		
/*		//compute the closest cluster for individual clusters
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
			cl.get(i).setMean();
			
			
		}*/
		//getting the list of points
		List<Point> pl =getPoints(cl);
		//setting the kd tree
		kdtree T=new kdtree(pl);
		//setting cluster and mean for each point
		for(int i=0;i<cl.size();i++) {
			setClusterPoint(cl.get(i));
			cl.get(i).setMean();
		}
		
		if(pass.equals("1")) {
			//compute the closest cluster
			for(int i=0;i<pl.size();i++) {
				T.getNN(pl.get(i), Double.MAX_VALUE);
				cl.get(i).setClosest(T.getNn().getPnt_nn().getC());
			}
		}
		else {
			for(int i=0;i<cl.size();i++) {
				List<Point> plist=cl.get(i).getRep();
				Cluster cc=null;
				double mindist=Double.MAX_VALUE;
				for(int j=0;j<plist.size();j++) {
					T.getNN(plist.get(j), Double.MAX_VALUE);
					Point cp=T.getNn().getPnt_nn();
					double dist=getdist(plist.get(j),cp);
					if(dist<mindist)
						cc=cp.getC();
						
				}
				cl.get(i).setClosest(cc);
			}
		}
		
		
		
		//setting the heap
		PriorityQueue<Cluster> Q=new PriorityQueue<Cluster>(new Comparator<Cluster>() {

			@Override
			public int compare(Cluster o1, Cluster o2) {
		
					// TODO Auto-generated method stub
					return Double.compare(o1.getMin_distance(), o2.getMin_distance());
			}
		});
		//adding to the priority queue
		for(int h=0;h<cl.size();h++) {
			Q.add(cl.get(h));
		}
		//set the cluster of each point
		System.out.println("Starting to compute CURE");
        try {
			computeCluster(k, c, alpha, Q,T);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cl=getClusters(Q);
		int counter=1;
		int reducer_id=context.getTaskAttemptID().getTaskID().getId();
		for(int i=0;i<cl.size();i++) {
			List<Point> rl=cl.get(i).getRep();
			for(int j=0;j<rl.size();j++) {
				context.write(new Text(reducer_id+"_"+counter), new Text(rl.get(j).toString()));
			}
			counter++;
			
		}
		
	
	}

	public void computeCluster(int k, int c, double alpha, PriorityQueue<Cluster> Q,kdtree T) throws Exception {

		while(Q.size()>k) {
			Cluster u=Q.poll();
			Cluster v=u.getClosest();
			//delete v from priority queue
			Q.remove(v);
			//delete u and v
			T.delNode(u.getRep());
			T.delNode(v.getRep());
			
			Cluster w=merge(u,v,c,alpha);
			
			//add the points from w
			T.insertNode(w.getRep());
			
			w.setClosest(Q.peek());
			Iterator<Cluster> it=Q.iterator();
			List<Cluster> mod_cl=new ArrayList<>();
			while(it.hasNext()) {
				Cluster x=it.next();
				if(Q.size()==1) {
					w.setClosest(x);
					x.setClosest(w);
					relocate(it, mod_cl, x);
					break;	
				}
				if(getdistCluster(w,x)<getdistCluster(w,w.getClosest()))
						w.setClosest(x);
				if(x.getClosest()==u || x.getClosest()==v) {
					
					if(getdistCluster(x, x.getClosest())<getdistCluster(w, x)) {
						x.setClosest(getClosestCluster(T,x,getdistCluster(w, x)));
					}
						
					else
						x.setClosest(w);
				relocate(it, mod_cl, x);			
				}
				else if(getdistCluster(x, x.getClosest())>getdistCluster(w,x))
				{
					x.setClosest(w);
					relocate(it, mod_cl, x);
				}	
			}
			for(int m=0;m<mod_cl.size();m++) {
				Q.add(mod_cl.get(m));
			}
			Q.add(w);	
		}
	}
	
	public static void relocate(Iterator<Cluster> it, List<Cluster> mod_cl, Cluster x) {
		mod_cl.add(x);
		it.remove();
	}

	private static Cluster getClosestCluster(kdtree t, Cluster x, double d) {
		// TODO Auto-generated method stub
		double min_dist=Double.MAX_VALUE;
		Cluster closest=null;
		List<Point> pl=x.getRep();
		for(int i=0;i<pl.size();i++) {
			t.getNN(pl.get(i), d);
			double dist=getdist(t.getNn().getPnt_nn(),pl.get(i));
			if(dist<=min_dist) {
				min_dist=dist;
				closest=t.getNn().getPnt_nn().getC();
				
			}
				
		}
		return closest;
	}

	public static double getdistCluster(Cluster w, Cluster x) {
		// TODO Auto-generated method stub
		double minDist=Double.MAX_VALUE;
		List<Point> set1=w.getRep();
		List<Point> set2=x.getRep();
		for(int i=0;i<set1.size();i++) {
			for(int j=0;j<set2.size();j++) {
				double dist=getdist(set1.get(i),set2.get(j));
				if(minDist>dist)
					minDist=dist;
			}
		}
		
		
		return minDist;
	}
	
	private Cluster merge(Cluster u, Cluster v,int c,double alpha) throws Exception {
		// TODO Auto-generated method stub
		List<Point> pl=new ArrayList<>();
		List<Point> temp=new ArrayList<>();
		pl.addAll(u.getRep());
		pl.addAll(v.getRep());
		Cluster w=new Cluster(null, pl, 0);
		//setting the mean
		w.setMean();
		//getting the scattered points
		if(pl.size()>c) {

			for(int i=0;i<c;i++) {
				double maxDist=0;
				Point maxPoint=null;
				for(int j=0;j<pl.size();j++) {
					double minDist=0;
					if(i==0) {
						minDist=getdistScatter(w.getMean(),pl.get(j),alpha);
					}
					else {
						minDist=Double.MAX_VALUE;
						if(temp.contains(pl.get(j)))
							continue;
						for(int k=0;k<temp.size();k++) {
							if(!temp.contains(pl.get(j))) {
								double dist=getdistScatter(temp.get(k),pl.get(j),alpha);
								if(dist<=minDist)
									minDist=dist;
							}
							
						}
					}
					if(minDist>=maxDist) {
						maxDist=minDist;
						maxPoint=pl.get(j);
					}
					
				}
				if(maxPoint!=null)
				temp.add(maxPoint);
			}	
		}
		else {
			temp=pl;
		}
		
		for(int i=0;i<temp.size();i++) {
			Point p=temp.get(i);
			double[] coord=getScatterPointCoord(p, alpha);
			double xcoord=coord[0]+alpha*(w.getMean().getX()-coord[0]);
			double ycoord=coord[1]+alpha*(w.getMean().getY()-coord[1]);
			p.setX(xcoord);
			p.setY(ycoord);
		}
		w.setRep(temp);
		setClusterPoint(w);
		w.setMean();
		return w;
		
		
	}
	private static double[] getScatterPointCoord(Point point,double alpha) {
		double[] scatter =new double[2];
		double sx=(point.getX()-alpha*(point.getC().getMean().getX()))/(1-alpha);
		double sy=(point.getY()-alpha*(point.getC().getMean().getY()))/(1-alpha);
		scatter[0]=sx;
		scatter[1]=sy;
		return scatter;
		
	}

	private static double getdistScatter(Point mean, Point point, double alpha) {
		// TODO Auto-generated method stub
		double sx=(point.getX()-alpha*(point.getC().getMean().getX()))/(1-alpha);
		double sy=(point.getY()-alpha*(point.getC().getMean().getY()))/(1-alpha);
		double dx=mean.getX()-sx;
		double dy=mean.getY()-sy;
		return Math.sqrt(dx*dx+dy*dy);
	}
	
	
	private static double getdist(Point p1, Point p2) {
		double dx=p1.getX()-p2.getX();
		double dy=p1.getY()-p2.getY();
		return Math.sqrt(dx*dx+dy*dy);
	}

	public static void setClusterPoint(Cluster c) {
		if(c.getRep()!=null) {
			List<Point> pl=c.getRep();
			for(int i=0;i<pl.size();i++) {
				pl.get(i).setC(c);
			}
		}
		
	}
	public List<Cluster> getClusters(PriorityQueue<Cluster> Q){
		List<Cluster> cl=new ArrayList<>();
		Iterator<Cluster> it=Q.iterator();
		while(it.hasNext()) {
			cl.add(it.next());
		}
		return cl;
	}
	
	public static List<Point> getPoints(List<Cluster> cl){
		List<Point> pl=new ArrayList<>();
		for(int i=0;i<cl.size();i++) {
			pl.addAll(cl.get(i).getRep());
		}
		return pl;
		
	}
	public static Cluster merge(PriorityQueue<Cluster> Q,kdtree T,int k_merge,int c,double alpha,List<Cluster> clist) throws Exception {
		//get the list of clusters to be merged 
		if(Q.size()>=k_merge*2) {
			int counter=1;
			while(counter++<=k_merge*2) {
				Cluster cluster=Q.poll();
				Q.remove(cluster.getClosest());
				clist.add(cluster);
				clist.add(cluster.getClosest());
			}
		}else
		{
			throw new Exception("Not enough cluster to be merged");
		}
		//retrive all the points 
		List<Point> pl=new ArrayList<>();
		for(int i=0;i<clist.size();i++) {
			pl.addAll(clist.get(i).getRep());
		}
		Cluster w=new Cluster(null, pl, 0);
		//setting the mean
		w.setMean();
		List<Point> temp=new ArrayList<>();
		//getting the scattered points
		if(pl.size()>c) {

			for(int i=0;i<c;i++) {
				double maxDist=0;
				Point maxPoint=null;
				for(int j=0;j<pl.size();j++) {
					double minDist=0;
					if(i==0) {
						minDist=getdistScatter(w.getMean(),pl.get(j),alpha);
					}
					else {
						minDist=Double.MAX_VALUE;
						if(temp.contains(pl.get(j)))
							continue;
						for(int k=0;k<temp.size();k++) {
							if(!temp.contains(pl.get(j))) {
								double dist=getdistScatter(temp.get(k),pl.get(j),alpha);
								if(dist<=minDist)
									minDist=dist;
							}
							
						}
					}
					if(minDist>=maxDist) {
						maxDist=minDist;
						maxPoint=pl.get(j);
					}
					
				}
				if(maxPoint!=null)
				temp.add(maxPoint);
			}	
		}
		else {
			temp=pl;
		}
		
		for(int i=0;i<temp.size();i++) {
			Point p=temp.get(i);
			double[] coord=getScatterPointCoord(p, alpha);
			double xcoord=coord[0]+alpha*(w.getMean().getX()-coord[0]);
			double ycoord=coord[1]+alpha*(w.getMean().getY()-coord[1]);
			p.setX(xcoord);
			p.setY(ycoord);
		}
		w.setRep(temp);
		setClusterPoint(w);
		w.setMean();
		//remove the points from kd_tree
		for(int i=0;i<clist.size();i++) {
			T.delNode(clist.get(i).getRep());
		}
		//add w's rep points to T
		T.insertNode(w.getRep());
		
		return w;
		
		
	}
	public void computeClusterkMerge(int k, int c, double alpha, PriorityQueue<Cluster> Q,kdtree T) throws Exception {

		while(Q.size()>k) {
			List<Cluster> merge_cl=new ArrayList<>();
			Cluster w=merge(Q,T,3,c,alpha,merge_cl);
			w.setClosest(Q.peek());
			Iterator<Cluster> it=Q.iterator();
			List<Cluster> mod_cl=new ArrayList<>();
			while(it.hasNext()) {
				Cluster x=it.next();
				if(Q.size()==1) {
					w.setClosest(x);
					x.setClosest(w);
					relocate(it, mod_cl, x);
					break;	
				}
				if(getdistCluster(w,x)<getdistCluster(w,w.getClosest()))
						w.setClosest(x);
				if(merge_cl.contains(x.getClosest())) {
					
					if(getdistCluster(x, x.getClosest())<getdistCluster(w, x)) {
						x.setClosest(getClosestCluster(T,x,getdistCluster(w, x)));
					}
						
					else
						x.setClosest(w);
				relocate(it, mod_cl, x);			
				}
				else if(getdistCluster(x, x.getClosest())>getdistCluster(w,x))
				{
					x.setClosest(w);
					relocate(it, mod_cl, x);
				}	
			}
			for(int m=0;m<mod_cl.size();m++) {
				Q.add(mod_cl.get(m));
			}
			Q.add(w);	
		}
	}

}
