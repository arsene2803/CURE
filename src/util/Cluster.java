package util;

import java.util.Comparator;
import java.util.List;

public class Cluster implements Comparator<Cluster> {
	
	Cluster closest=null;
	Point mean;
	double min_distance;

	List<Point> rep=null;
	public Cluster(Cluster closest, List<Point> rep, double min_distance ) {
		super();
		this.closest = closest;
		this.rep=rep;
		this.min_distance=min_distance;
	}
	public Cluster getClosest() {
		return closest;
	}
	public void setClosest(Cluster closest) {
		this.closest = closest;
		this.min_distance=getdistCluster(this, this.closest);
	}
	public Point getMean() {
		return mean;
	}
	public void setMean() {
		if(mean==null)
			mean=new Point();
		
		double sum_x=0,sum_y=0;
		
		if(rep.size()>=1) {
			for(int i=0;i<rep.size();i++) {
				sum_x+=rep.get(i).getX();
				sum_y+=rep.get(i).getY();
			}
		}
		mean.setX(sum_x/rep.size());
		mean.setY(sum_y/rep.size());
		
		
	}
	public List<Point> getRep() {
		return rep;
	}
	public void setRep(List<Point> rep) {
		this.rep = rep;
	}
	private double getdist(Point p1, Point p2) {
		double dx=p1.getX()-p2.getX();
		double dy=p1.getY()-p2.getY();
		return Math.sqrt(dx*dx+dy*dy);
	}
	private double getdistCluster(Cluster w, Cluster x) {
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
	public double getMin_distance() {
		
		return min_distance;
	}
	public void setMin_distance(double min_distance) {
		this.min_distance = min_distance;
	}
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
	

}
