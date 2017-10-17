package cure;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import kd_tree.kdtree;
import util.Cluster;
import util.Point;

public class testRun {
	public static void run(List<String> values) {
		int k=2,c=56;
		double alpha=0.8;
		//each point will be individual cluster
		List<Cluster> cl=new ArrayList<>();
		for(String val:values) {
			int i=0;
			double[] coord=new double[2]; 
			String line=val.toString();
			StringTokenizer st=new StringTokenizer(line);
			while(st.hasMoreTokens()) {
				coord[i++]=Double.parseDouble(st.nextToken(","));
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
			cl.get(i).setMean();
			
		}
		//checking out the values
		for(int i=0;i<cl.size();i++) {
			System.out.println("X: "+cl.get(i).getRep().get(0).getX());
			System.out.println("Y: "+cl.get(i).getRep().get(0).getY());
			System.out.println("closest: X"+cl.get(i).getClosest().getRep().get(0).getX());
			System.out.println("closest: Y"+cl.get(i).getClosest().getRep().get(0).getY());
			System.out.println("Minimal distance: "+cl.get(i).getMin_distance());
			
		}
		//setting the heap
		PriorityQueue<Cluster> Q=new PriorityQueue<Cluster>(new Comparator<Cluster>() {

			@Override
			public int compare(Cluster o1, Cluster o2) {
				// TODO Auto-generated method stub
				if(o1.getMin_distance()>o1.getMin_distance()) {
					return 1;
				}
				else if(o1.getMin_distance()<o1.getMin_distance()) {
					return -1;
				}
				else
					return 0;
			}
		});
		//getting the list of points
		List<Point> pl =getPoints(cl);
		//setting the kd tree
		kdtree T=new kdtree(pl);
		//set the cluster of each point
		for(int i=0;i<cl.size();i++) {
			setClusterPoint(cl.get(i));
			Q.add(cl.get(i));
		}
		
		while(Q.size()>k) {
			System.out.println(Q.size());
			Cluster u=Q.poll();
			Cluster v=u.getClosest();
			//delete v from priority queue
			Q.remove(v);
			Cluster w=merge(u,v,c,alpha);
			//rebuild the kdtree,need to figure out how to delete again 
			cl=getClusters(Q);
			pl=getPoints(cl);
			T=new kdtree(pl);
			w.setClosest(Q.peek());
			Iterator<Cluster> it=Q.iterator();
			while(it.hasNext()) {
				Cluster x=it.next();
				if(getdistCluster(w,x)<getdistCluster(w,w.getClosest()))
						w.setClosest(x);
				if(x.getClosest()==u || x.getClosest()==v) {
					if(getdistCluster(x, x.getClosest())<getdistCluster(w, x)) 
						x.setClosest(getClosestCluster(T,x,getdistCluster(w, x)));
					else
						x.setClosest(w);
				//relocate x
					relocate(Q,x);			
				}
				else if(getdistCluster(x, x.getClosest())>getdistCluster(w,x))
				{
					x.setClosest(w);
					relocate(Q,x);
				}	
			}
			Q.add(w);	
			
		}
		cl=getClusters(Q);
		System.out.println("Done clustering");
		
		
	}
	
	private static void relocate(PriorityQueue<Cluster> q, Cluster x) {
		// TODO Auto-generated method stub
		q.remove(x);
		q.add(x);
		
	}
	
	private static Cluster getClosestCluster(kdtree t, Cluster x, double d) {
		// TODO Auto-generated method stub
		double min_dist=Double.MAX_VALUE;
		Cluster closest=null;
		List<Point> pl=x.getRep();
		for(int i=0;i<pl.size();i++) {
			t.getNN(pl.get(i), d);
			double dist=getdist(t.getNn().getPnt_nn(),pl.get(i));
			if(dist<min_dist) {
				min_dist=dist;
				closest=t.getNn().getPnt_nn().getC();
				
			}
				
		}
		return closest;
	}
	public static List<Cluster> getClusters(PriorityQueue<Cluster> Q){
		List<Cluster> cl=new ArrayList<>();
		Iterator<Cluster> it=Q.iterator();
		while(it.hasNext()) {
			cl.add(it.next());
		}
		return cl;
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
	private static double getdist(Point p1, Point p2) {
		double dx=p1.getX()-p2.getX();
		double dy=p1.getY()-p2.getY();
		return Math.sqrt(dx*dx+dy*dy);
	}
	private static double getdistScatter(Point mean, Point point, double alpha) {
		// TODO Auto-generated method stub
		double sx=(point.getX()-alpha*(point.getC().getMean().getX()))/(1-alpha);
		double sy=(point.getY()-alpha*(point.getC().getMean().getY()))/(1-alpha);
		double dx=mean.getX()-sx;
		double dy=mean.getY()-sy;
		return Math.sqrt(dx*dx+dy*dy);
	}
	
	private static Cluster merge(Cluster u, Cluster v,int c,double alpha) {
		// TODO Auto-generated method stub
		List<Point> pl=new ArrayList();
		List<Point> temp=new ArrayList();
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
						minDist=0;
						for(int k=0;k<temp.size();k++) {
							if(!temp.contains(pl.get(j))) {
								double dist=getdistScatter(temp.get(k),pl.get(j),alpha);
								if(dist<minDist)
									minDist=dist;
							}
							
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
		w.setRep(temp);
		setClusterPoint(w);
		return w;
		
		
	}
	public static List<Point> getPoints(List<Cluster> cl){
		List<Point> pl=new ArrayList<>();
		for(int i=0;i<cl.size();i++) {
			pl.addAll(cl.get(i).getRep());
		}
		return pl;
		
	}
	public static void setClusterPoint(Cluster c) {
		if(c.getRep()!=null) {
			List<Point> pl=c.getRep();
			for(int i=0;i<pl.size();i++) {
				pl.get(i).setC(c);
			}
		}
		
	}
	
	public static void main(String[] args) {
		List<String> input=new ArrayList<>();
		input.add("1,2");
		input.add("3,4");
		input.add("100,3");
		run(input);
	}

}