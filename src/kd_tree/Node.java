package kd_tree;

import util.Cluster;
import util.Point;

public class Node {
	int depth;
	Point pnt;
	Node L,R;
	Cluster clr;
	
	public Node(int depth) {
		this.depth=depth;
		this.L=null;
		this.R=null;
	}
	public boolean isLeaf() {
		return (L==null)|(R==null);
	}
	

}
