package cure;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.opencsv.CSVReader;

import util.Cluster;
import util.Point;

public class AssignCluster {
	public static void main(String[] args) {
		List<String> clusterPoints=new ArrayList<>();
		List<Cluster> cl=new ArrayList<>();
		//read from csv file
		List<String[]> inp=readCSV(args[1],'\t');

		for(int i=0;i<inp.size();i++) {
			clusterPoints.add(inp.get(i)[0]+"|"+inp.get(i)[1]);
		}
		Map<String,List<Point>> hmap=new HashMap<>();
		for(String val: clusterPoints) {
			int i=0;
			StringTokenizer st=new StringTokenizer(val.toString(),"|");
			String keyValue=st.nextToken();
			String value=st.nextToken();
			st=new StringTokenizer(value,",");
			double[] coord=new double[2];
			while(st.hasMoreTokens()) {
				coord[i++]=Double.parseDouble(st.nextToken().replace("\"", ""));
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
		getAccuracy(args[0], cl);
		
		
	}
	public static void getAccuracy(String inputFileName, List<Cluster> cl) {
		List<String[]> inp;
		//read the input 
		inp=readCSV(inputFileName,',');
		List<Point> pl=new ArrayList<>();
		List<String> input=new ArrayList<>(); 
		for(int i=0;i<inp.size();i++) {
			input.add(inp.get(i)[0]+","+inp.get(i)[1]);
		}
		for(String val:input) {
			int i=0;
			double[] coord=new double[2]; 
			String line=val.toString();
			StringTokenizer st=new StringTokenizer(line);
			while(st.hasMoreTokens()) {
				coord[i++]=Double.parseDouble(st.nextToken(","));
			}
			pl.add(new Point(coord[0],coord[1]));
		
		}
		Map<Cluster,List<Point>> cmap=new HashMap<>();
		//assign clusters for individual points
		for(int i =0;i<pl.size();i++) {
			double minDist=Double.MAX_VALUE;
			Point p=pl.get(i);
			for(int j=0;j<cl.size();j++) {
				Cluster c=cl.get(j);
				List<Point> rep=c.getRep();
				for(int k=0;k<rep.size();k++) {
					double dist=getdist(rep.get(k), p);
					if(dist<=minDist) {
						minDist=dist;
						p.setC(c);
					}
					
				}
			}
			if(!cmap.containsKey(p.getC())) {
				List<Point> temp=new ArrayList<>();
				temp.add(p);
				cmap.put(p.getC(), temp);
			}
			else
			{
				cmap.get(p.getC()).add(p);
			}
		}
		//Write the results to a file
		fileWrite(cmap,"output");
		System.out.println("Calculating the silhoute ecofficient");
		List<Double> sl_cofficient=new ArrayList<>();
		for(int i=0;i<pl.size();i++) {
			//calculating ai
			Point p=pl.get(i);
			List<Point> plist=cmap.get(p.getC());
			double sum_d=0;
			for(int k=0;k<plist.size();k++) {
				Point t=plist.get(k);
				if(p!=t) {
					sum_d+=getdist(p, t);
				}
			}
			double a_i=sum_d/(plist.size()-1);
			//iterating through hashmap
			double min_avg_d=Double.MAX_VALUE;
			for(Cluster cluster:cmap.keySet()) {
				if(p.getC()!=cluster) {
					sum_d=0;
					List<Point> tl=cmap.get(cluster);
					for(int u=0;u<tl.size();u++) {
						sum_d+=getdist(p, tl.get(u));
					}
					double avg_d=sum_d/tl.size();
					if(avg_d<=min_avg_d) {
						min_avg_d=avg_d;
					}
					
				}
	
			}
			double si=(min_avg_d-a_i)/(Math.max(min_avg_d, a_i));
			sl_cofficient.add(si);
			
		}
		double sum_e=0;
		for(int v=0;v<sl_cofficient.size();v++) {
			sum_e+=sl_cofficient.get(v);
		}
		System.out.println("Average silhoute ecofficent is "+sum_e/sl_cofficient.size());
	}
	private static void fileWrite(Map<Cluster, List<Point>> cmap,String fileName) {
		// TODO Auto-generated method stub
		int counter=0;
		try {
			FileWriter fw=new FileWriter(fileName);
			BufferedWriter bw=new BufferedWriter(fw);
			for(List<Point> pl:cmap.values()) {
				for(int i=0;i<pl.size();i++) {
					bw.write(counter+"\n");
				}
				counter++;
			}
			bw.close();
			fw.close();
				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
			
    }
		
	private static double getdist(Point p1, Point p2) {
		double dx=p1.getX()-p2.getX();
		double dy=p1.getY()-p2.getY();
		return Math.sqrt(dx*dx+dy*dy);
	}
	private static void run(List<String> input) {
		
		// TODO Auto-generated method stub
		//assign clusters
		
		
	}
	public static List<String[]> readCSV(String fileName,char delimiter){
		try {
			CSVReader reader=new CSVReader(new FileReader(fileName),delimiter);
			List<String[]> values=reader.readAll();
			reader.close();
			return values;
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
