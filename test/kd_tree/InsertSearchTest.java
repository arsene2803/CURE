package kd_tree;

import java.util.ArrayList;
import java.util.List;

import Reducer.Curereducer;
import util.Cluster;
import util.Point;

public class InsertSearchTest {
	
	public static void main(String[] args) {
		List<Point> pList1=new ArrayList<>();
		pList1.add(new Point(30, 40));
		pList1.add(new Point(5,25));
		pList1.add(new Point(10,25));
		Cluster u=new Cluster(null, pList1,0);
		List<Point> pList2=new ArrayList<>();
		pList2.add(new Point(70,70));
		pList2.add(new Point(50,30));
		Cluster v=new Cluster(null, pList2,0);
		List<Point> pList3=new ArrayList<>();
		pList3.add(new Point(35,40));
		pList3.add(new Point(35,50));
		Cluster z=new Cluster(null, pList3,0);
		List<Cluster> cl=new ArrayList<>();
		cl.add(u);
		cl.add(v);
		cl.add(z);
		Curereducer.setClusterPoint(u);
		Curereducer.setClusterPoint(v);
		Curereducer.setClusterPoint(z);
		kdtree tree= new kdtree(Curereducer.getPoints(cl));
		kdtree.NN nn=tree.getNN(pList1.get(0), Double.MAX_VALUE);//30,40
		System.out.println(nn.pnt_nn.getX());
		System.out.println(nn.pnt_nn.getY());
		System.out.println("Checking find min functionality");
		tree.root=tree.delNode(tree.root, pList3.get(0));//35,40
		tree.root=tree.delNode(tree.root, pList2.get(0));//70,70
		kdtree.NN nn1=tree.getNN(pList1.get(0), Double.MAX_VALUE);//30,40
		System.out.println(nn1.pnt_nn.getX());
		System.out.println(nn1.pnt_nn.getY());
		
		System.out.println(tree);
	}

}
