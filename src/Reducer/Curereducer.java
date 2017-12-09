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
		kdtree T=new kdtree();
		T.insertNode(pl);
		//setting cluster and mean for each point
		for(int i=0;i<cl.size();i++) {
			setClusterPoint(cl.get(i));
			cl.get(i).setMean();
		}
		
		if(pass.equals("1")) {
			//compute the closest cluster
			computeClosestFirstPass(cl, pl, T);
		}
		else {
			computeClosestSecondPass(cl, T);
		}
		
		
		
		PriorityQueue<Cluster> Q = initializePriorityQueue(cl);
		//set the cluster of each point
		System.out.println("Starting to compute CURE");
        try {
			//computeCluster(k, c, alpha, Q,T);
        	computeClusterHashMap(k, c, alpha, Q, T, cl);
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
	public static PriorityQueue<Cluster> initializePriorityQueue(List<Cluster> cl) {
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
		return Q;
	}
	public void computeClosestFirstPass(List<Cluster> cl, List<Point> pl, kdtree T) {
		for(int i=0;i<pl.size();i++) {
			T.getNN(pl.get(i), Double.MAX_VALUE);
			cl.get(i).setClosest(T.getNn().getPnt_nn().getC());
		}
	}
	public static void computeClosestSecondPass(List<Cluster> cl, kdtree T) {
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

	public void computeBruteCure(int k, int c, double alpha, PriorityQueue<Cluster> Q,kdtree T) throws Exception {

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
	
	private static Cluster merge(Cluster u, Cluster v,int c,double alpha) throws Exception {
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
		
		List<Point> res=new ArrayList<>();

		for(int i=0;i<temp.size();i++) {
			
			Point p=temp.get(i);
			double[] coord=getScatterPointCoord(p, alpha);
			double xcoord=coord[0]+alpha*(w.getMean().getX()-coord[0]);
			double ycoord=coord[1]+alpha*(w.getMean().getY()-coord[1]);
			res.add(new Point(xcoord,ycoord));
		
		}
		w.setRep(res);
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
	public static List<Cluster> getClusters(PriorityQueue<Cluster> Q){
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
		int counter=1;
		while(counter++<=k_merge) {
				Cluster cluster=Q.poll();
				Q.remove(cluster.getClosest());
				clist.add(cluster);
				clist.add(cluster.getClosest());
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
		//remove the points from kd_tree
		for(int i=0;i<clist.size();i++) {
			T.delNode(clist.get(i).getRep());
		}
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
		List<Point> res=new ArrayList<>();
		for(int i=0;i<temp.size();i++) {
			Point p=temp.get(i);
			double[] coord=getScatterPointCoord(p, alpha);
			double xcoord=coord[0]+alpha*(w.getMean().getX()-coord[0]);
			double ycoord=coord[1]+alpha*(w.getMean().getY()-coord[1]);
			res.add(new Point(xcoord,ycoord));
		}
		w.setRep(res);
		setClusterPoint(w);
		w.setMean();
		
		//add w's rep points to T
		T.insertNode(w.getRep());
		
		return w;
		
		
	}
	public void computeClusterkMerge(int k,int k_merge, int c, double alpha, PriorityQueue<Cluster> Q,kdtree T) throws Exception {

		while(Q.size()>k && Q.size()>=k_merge*2) {
			List<Cluster> merge_cl=new ArrayList<>();
			Cluster w=merge(Q,T,k_merge,c,alpha,merge_cl);
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
						x.setClosest(getClosestCluster(T,x,Double.MAX_VALUE));
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
	public static void computeClusterHashMap(int k, int c, double alpha, PriorityQueue<Cluster> Q,kdtree T,List<Cluster> cl) throws Exception {
		System.out.println("Computing clusters using CURE using HASHMAP optimization");
		//build a hashmap 
		Map<Cluster,List<Cluster>> hmap=new HashMap<>();
		for(int i=0;i<cl.size();i++) {
			Cluster cluster_key=cl.get(i).getClosest();
			if(!hmap.containsKey(cluster_key)) {
				List<Cluster> temp_cl=new ArrayList<>();
				temp_cl.add(cl.get(i));
				hmap.put(cluster_key, temp_cl);
				
			}
			else {
				hmap.get(cluster_key).add(cl.get(i));
			}
		}
		int num_pnts_outlier=Q.size()*1/3;
		boolean check_outlier=false;
		//processing begins now
		while(Q.size()>k) {
			System.out.println(Q.size());
			Cluster u=Q.poll();
			Cluster v=u.getClosest();
			Cluster u_closest=getClosestCluster(T, u, Double.MAX_VALUE);
			
			while(v!=u_closest) {				
				updateHMap(hmap, u,v);
				//add entry
				addEntryHMap(hmap, u, u_closest);
				u.setClosest(u_closest);
				if(v.getClosest()==u) {
					Cluster v_closest=getClosestCluster(T, v, Double.MAX_VALUE);
					updateHMap(hmap, v,u);
					//add entry
					addEntryHMap(hmap, v, v_closest);
					v.setClosest(v_closest);
					Q.remove(v);
					Q.add(v);
				}
				//add it to the queue
				Q.add(u);
				u=Q.poll();
				v=u.getClosest();
				u_closest=getClosestCluster(T, u, Double.MAX_VALUE);	
			}
			
			//delete v from priority queue
			Q.remove(v);
			//remove the points from the kdtree before the merge
			T.delNode(u.getRep());
			T.delNode(v.getRep());
			Cluster w=merge(u,v,c,alpha);
			//add the points from w
			T.insertNode(w.getRep());

			//removing u and v from the values end of HashMap
			Cluster ckey=u.getClosest();
			updateHMap(hmap, u, ckey);
			ckey=v.getClosest();
			updateHMap(hmap, v, ckey);
			//getting the clusters which needs to be updated because of the merging
			List<Cluster> mod_cl=new ArrayList<>();
			//getting the clusters which had u and v as the closest ones
			//removing u and v from the hmap
			removeHMapEntry(hmap, u, mod_cl);
			removeHMapEntry(hmap, v, mod_cl);
			/*List<Cluster> cur_cl=getClusters(Q);
			cur_cl.add(w);
			T=new kdtree(getPoints(cur_cl));*/
			//update the clusters affected due to merge
			for(int i=0;i<mod_cl.size();i++) {
				
				Cluster cluster=mod_cl.get(i);
				updateHMap(hmap, cluster, cluster.getClosest());
				if(getdistCluster(cluster, cluster.getClosest())<getdistCluster(w,cluster)) {
					//need to optimise this part
					cluster.setClosest(getClosestCluster(T,cluster,Double.MAX_VALUE));
				}
					
				else
					cluster.setClosest(w);
			
				Cluster key=cluster.getClosest();
				addEntryHMap(hmap, cluster, key);
			}
			Cluster closest=getClosestCluster(T, w, Double.MAX_VALUE);
			
			if(closest.getMin_distance()>=getdistCluster(w,closest)) {
				ckey=closest.getClosest();
				//remove entry
				updateHMap(hmap, closest, ckey);
				closest.setClosest(w);
				//add entry
				addEntryHMap(hmap, closest, w);
				//adding it to modified list
				if(!mod_cl.contains(closest))
					mod_cl.add(closest);
				//get all the clusters which are closest 
				List<Cluster> clist=hmap.get(ckey);
				for(int i=0;i<clist.size();i++) {
					Cluster m=clist.get(i);
					if(m.getMin_distance()<=getdistCluster(w,closest))
					{
						ckey=m.getClosest();
						//remove entry
						updateHMap(hmap, m, ckey);
						m.setClosest(w);
						//add entry
						addEntryHMap(hmap, m, w);
						//adding it to modified list
						if(!mod_cl.contains(m))
							mod_cl.add(m);
					}
					
					
				}
			}
			w.setClosest(closest);
			//updating the hashmap for the merged cluster
			addEntryHMap(hmap, w, w.getClosest());
			
			//update the priority queue
			for(int i=0;i<mod_cl.size();i++) {
				Cluster cluster=mod_cl.get(i);
				Q.remove(cluster);
				Q.add(cluster);
			}
			Q.add(w);
			/*if(Q.size()<=num_pnts_outlier && !check_outlier) {
				check_outlier=true;
				remove_outLiers(Q);
			}*/
		}
	}
	private static void remove_outLiers(PriorityQueue<Cluster> q) {
		// TODO Auto-generated method stub
		Iterator<Cluster> it=q.iterator();
		while(it.hasNext()) {
			Cluster c=it.next();
			if(c.getRep().size()<=2)
				it.remove();
			
		}
		
	}
	public static void addEntryHMap(Map<Cluster, List<Cluster>> hmap, Cluster cluster, Cluster key) {
		if(hmap.containsKey(key)) {
			if(!hmap.get(key).contains(cluster))
				hmap.get(key).add(cluster);
		}
		else
		{
			List<Cluster> temp=new ArrayList<>();
			temp.add(cluster);
			hmap.put(key,temp);
		}
	}
	public static void updateHMap(Map<Cluster, List<Cluster>> hmap, Cluster u, Cluster ckey) {
		//updating the hashmap entry
		if(hmap.containsKey(ckey)) {
			hmap.get(ckey).remove(u);
		}
	}
	public static void removeHMapEntry(Map<Cluster, List<Cluster>> hmap, Cluster u, List<Cluster> mod_cl) {
		if(hmap.containsKey(u)) {
			mod_cl.addAll(hmap.get(u));
			hmap.remove(u);
		}
	}

}
