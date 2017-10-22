package kd_tree;

import java.util.ArrayList;
import java.util.List;

import Reducer.Curereducer;
import util.Cluster;
import util.Point;

public class InsertSearchTest {
	
	public static void main(String[] args) {
		List<Point> pList1=new ArrayList<>();
		pList1.add(new Point(2, 3));
		pList1.add(new Point(3,4));
		pList1.add(new Point(1,2));
		Cluster u=new Cluster(null, pList1,0);
		List<Point> pList2=new ArrayList<>();
		pList2.add(new Point(10,12));
		pList2.add(new Point(50,40));
		Cluster v=new Cluster(null, pList2,0);
		List<Point> pList3=new ArrayList<>();
		pList3.add(new Point(100,112));
		Cluster z=new Cluster(null, pList3,0);
		List<Cluster> cl=new ArrayList<>();
		cl.add(u);
		cl.add(v);
		cl.add(z);
		Curereducer.setClusterPoint(u);
		Curereducer.setClusterPoint(v);
		Curereducer.setClusterPoint(z);
		kdtree tree= new kdtree(Curereducer.getPoints(cl));
		System.out.println(tree.root.pnt.getX());
		System.out.println(tree.root.pnt.getY());
		kdtree.NN nn=tree.getNN(pList3.get(0), Double.MAX_VALUE);
		System.out.println(nn.pnt_nn.getX());
		System.out.println(nn.pnt_nn.getY());
		
	}

}
