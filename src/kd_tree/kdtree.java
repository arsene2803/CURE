package kd_tree;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import util.Point;

public class kdtree {
	int max_depth=0;
	Node root;
	NN nn;
	
	
	public kdtree(List<Point> points) {
		max_depth= (int)Math.ceil(Math.log(points.size())/Math.log(2));
		build(root=new Node(0),points);
		nn=new NN(null);
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
			if(0<m)
			build((node.L=new Node(++depth)),copy(points,0,m));
			
			if(m+1<e)
			build((node.R=new Node(depth)),copy(points,m+1,e));
			
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
	//finds minimum for a node
	public Node findMin(Node root,int d,int depth) {
		if(root==null)
			return null;
		if((depth&1)==d) {
			if(root.L==null)
				return root;
			else
				return findMin(root.L,d,depth+1);
		}
		return minNode(root,findMin(root.L,d,depth+1),findMin(root.R,d,depth+1),d);
		
	}
	//wrapper for find
	public Node findMin(Node root,int d) {
		return findMin(root,d,0);
	}
	
	private Node minNode(Node x, Node y, Node z,int d) {
		// TODO Auto-generated method stub
		Node res=x;
		double res_cord=(d==0)?res.pnt.getX():res.pnt.getY();
		if(y!=null) {
			Point py=y.pnt;
			double py_cord=(d==0)?py.getX():py.getY();
			res=(py_cord<res_cord)?y:res;
			
		}
		if(z!=null) {
			Point pz=z.pnt;
			res_cord=(d==0)?res.pnt.getX():res.pnt.getY();
			double pz_cord=(d==0)?pz.getX():pz.getY();
			res=(pz_cord<res_cord)?z:res;
		}
		
		return res;
	}
	
	public Node delNode(Node root,Point p,int depth) {
		//given point is not present 
		if(root==null)
			return null;
		int d=depth&1;
		//check if the points are same
		if(root.pnt==p) {
			//if right child is not null
			if(root.R!=null) {
				//find min in the right subtree
				Node min=findMin(root.R, d,depth+1);
				//copy the points
				copyPoints(root,min);
				root.R=delNode(root.R,min.pnt,depth+1);
			}
			else if(root.L!=null) {
				//same as above
				Node min=findMin(root.L, d,depth+1);
				//copy the points
				copyPoints(root,min);
				root.R=delNode(root.L,min.pnt,depth+1);
			}
			else {
				root=null;
				return null;
			}
			
		}
		double p_coord=(d==0)?p.getX():p.getY();
		double r_coord=(d==0)?root.pnt.getX():root.pnt.getY();
		if(p_coord<r_coord)
			root.L=delNode(root.L,p,depth+1);
		else
			root.R=delNode(root.R,p,depth+1);
		return root;
		
	}
	
	public Node delNode(Node root,Point p) {
		root=delNode(root,p,0);
		return root ;
		
	}
	
	public Node delNode(List<Point> pl) {
		for(int i=0;i<pl.size();i++) {
			root=delNode(root,pl.get(i));
		}
		return root;
		
	}
	public Node insertNode(Node root,Point p,int depth) {
		
		if(root==null) {
			Node n= new Node(depth);
			n.pnt=p;
			return n;
		}
		
		int d=depth&1;
		//decide left or right subtree
		double p_coord=(d==0)?p.getX():p.getY();
		double r_coord=(d==0)?root.pnt.getX():root.pnt.getY();
		if(p_coord<r_coord)
			root.L=insertNode(root.L,p,depth+1);
		else
			root.R=insertNode(root.R,p,depth+1);
		
		return root;
			
	}
	
	public Node insertNode(Node root,Point p) {
		return insertNode(root,p,0);
	}
	
	public Node insertNode(List<Point> pl) {
		for(int i=0;i<pl.size();i++) {
			root=insertNode(root,pl.get(i));
		}
		return root;
	}

	private void copyPoints(Node root, Node min) {
		// TODO Auto-generated method stub
		root.pnt=min.pnt;
		
		
	}

	public static class NN{
		public Point getPnt_nn() {
			return pnt_nn;
		}

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
			if((dx==0 && dy==0)||(pnt_in.getC()!=null && (node.pnt.getC()!=null)&&(pnt_in.getC()==node.pnt.getC())))
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
		nn.min_dist=min_dist;
		nn.pnt_in=point;
		getNN(nn,root);
		return nn;
	}

	public NN getNn() {
		return nn;
	}


	private void getNN(NN nn, Node node) {
		// TODO Auto-generated method stub
			if(node==null)
				return;
			nn.update(node);
			double dist_hp=planedistnace(node,nn.pnt_in);
			//check the half space
			getNN(nn,(dist_hp<0)?node.L:node.R);
			//checking the other half
			if(Math.abs(dist_hp)<nn.min_dist)
				getNN(nn,(dist_hp<0)?node.R:node.L);

		
	}

	private double planedistnace(Node node, Point pnt_in) {
		// TODO Auto-generated method stub

		if((node.depth&1)==0)
			return pnt_in.getX()-node.pnt.getX();
		else
			return pnt_in.getY()-node.pnt.getY();
	}
	
}
