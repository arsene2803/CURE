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
	
}
