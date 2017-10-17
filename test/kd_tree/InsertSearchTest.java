package kd_tree;

import java.util.ArrayList;
import java.util.List;

import util.Point;

public class InsertSearchTest {
	
	public static void main(String[] args) {
		List<Point> pList=new ArrayList<>();
		pList.add(new Point(2, 3));
		pList.add(new Point(3,4));
		pList.add(new Point(1,2));
		kdtree tree= new kdtree(pList);
		System.out.println(tree.root.pnt.getX());
		System.out.println(tree.root.pnt.getY());
		kdtree.NN nn=tree.getNN(new Point(2,3), Double.MAX_VALUE);
		System.out.println(nn.pnt_nn.getX());
		System.out.println(nn.pnt_nn.getY());
		
	}

}
