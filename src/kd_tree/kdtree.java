package kd_tree;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import util.Point;

public class kdtree {
	int max_depth=0;
	Node root;
	
	public kdtree(List<Point> points) {
		max_depth= (int)Math.ceil(Math.log(points.size())/Math.log(2));
		build(root=new Node(0),points);
	}
	
	private void build(Node node, List<Point> points) {
		// TODO Auto-generated method stub
		final int e=points.size();
		final int m=e>>1;
		if(e>1) {
			
			int depth=node.depth;
			if((depth&1)==0)
				Collections.sort(points,new SORT_X());
			else
				Collections.sort(points,new SORT_Y());
			build((node.L=new Node(++depth)),copy(points,0,m));
			build((node.R=new Node(depth)),copy(points,m,e));
			
		}
		node.pnt=points.get(m);
		
	}
	
	private List<Point> copy(List<Point> points, int start, int end) {
		// TODO Auto-generated method stub
		return points.subList(start, end);
	}
	
	public static class SORT_X implements Comparator<Point>{

		@Override
		public int compare(Point o1, Point o2) {
			// TODO Auto-generated method stub
			return (o1.getX()>o2.getX())?1:(o1.getX()==o2.getX())?0:-1;
		}
		
	}
	
	public static class SORT_Y implements Comparator<Point>{

		@Override
		public int compare(Point o1, Point o2) {
			// TODO Auto-generated method stub
			return (o1.getY()>o2.getY())?1:(o1.getY()==o2.getY())?0:-1;
		}
		
	}
	public static class NN{
		Point pnt_in;
		Point pnt_nn=null;
		double min_dist=Double.MAX_VALUE;
		//constructor
		public NN(Point pnt_in, double min_dist) {
			super();
			this.pnt_in = pnt_in;
			this.min_dist = min_dist;
		}
		public NN(Point pnt_in) {
			super();
			this.pnt_in = pnt_in;
		}
		
		void update(Node node) {
			
			double dx=node.pnt.getX()-pnt_in.getX();
			double dy=node.pnt.getY()-pnt_in.getY();
			//checking if its the same point
			if((dx==0 && dy==0))
				return ;
			//checking whether it belongs to same cluster
			if(node.pnt.getC()!=null && pnt_in.getC()!=null)
			{
				if((node.pnt.getC()==pnt_in.getC()))
					return;
			}
			double cur_dist=Math.sqrt(dx*dx+dy*dy);
			if(cur_dist<min_dist) {
				pnt_nn=node.pnt;
				min_dist=cur_dist;
			}
		}		
		
	}
	
	public NN getNN(Point point,double min_dist) {
		NN nn=new NN(point,min_dist);
		getNN(nn,root);
		return nn;
	}

	private void getNN(NN nn, Node node) {
		// TODO Auto-generated method stub
		if(node.isLeaf()) {
			nn.update(node);
		}
		else {
			
			double dist_hp=planedistnace(node,nn.pnt_in);
			//check the half space
			getNN(nn,(dist_hp<0)?node.L:node.R);
			//checking the other half
			if(dist_hp<nn.min_dist)
				getNN(nn,(dist_hp<0)?node.R:node.L);
			
		}
		
	}

	private double planedistnace(Node node, Point pnt_in) {
		// TODO Auto-generated method stub

		if((node.depth&1)==0)
			return pnt_in.getX()-node.pnt.getX();
		else
			return pnt_in.getY()-node.pnt.getY();
	}
	
}
