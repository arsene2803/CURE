package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import Reducer.Curereducer;
import kd_tree.kdtree;

public class PostProcess {
	public static File[] getFiles(String outputPath) {
		File folder = new File(outputPath);
		File[] listOfFiles = folder.listFiles();
		return listOfFiles;
	}
	public static CSVWriter createCsvWrtier(String finalOutputPath) throws IOException {
		CSVWriter writer=new CSVWriter(new FileWriter(finalOutputPath),'\t');
		return writer;
	}
	public static void runSecondPass(String outputPath,String finalOutputPath) throws IOException {
		Map<String,List<Point>> hmap=new HashMap<>();
		File[] output=getFiles(outputPath);
		for(int i=0;i<output.length;i++) {
			File file=output[i];
			List<String[]> lines=readCSV(file.getPath(), '\t');
			for(int j=0;j<lines.size();j++) {
				int m=0;
				String[] line=lines.get(j);
				String key=line[0];
				double[] coord=new double[2];
				StringTokenizer st=new StringTokenizer(line[1]);
				while(st.hasMoreTokens()) {
					coord[m++]=Double.parseDouble(st.nextToken(","));
				}
				if(hmap.containsKey(key)) {
					hmap.get(key).add(new Point(coord[0], coord[1]));
				}
				else {
					List<Point> temp=new ArrayList<>();
					temp.add(new Point(coord[0], coord[1]));
					hmap.put(key, temp);
				}
			}
		}
		//need to set the number of clusters k,the shrinking factor alpha and the number of scattered points
		int c=56,k=10;
		double alpha=0.8;
		//each point will be individual cluster
		List<Cluster> cl=new ArrayList<>();
		//iterate through hash map
		for(List<Point> plist:hmap.values()) {
		cl.add(new Cluster(null,plist,0));
		}
		//getting the list of points
		List<Point> pl =Curereducer.getPoints(cl);
		//setting the kd tree
		kdtree T=new kdtree();
		T.insertNode(pl);
		//setting cluster and mean for each point
		for(int i=0;i<cl.size();i++) {
			Curereducer.setClusterPoint(cl.get(i));
			cl.get(i).setMean();
		}
		Curereducer.computeClosestSecondPass(cl, T);
		//initialize the priority queue
		PriorityQueue<Cluster> Q = Curereducer.initializePriorityQueue(cl);
		System.out.println("Starting to compute CURE secondPass");
        try {
			//computeCluster(k, c, alpha, Q,T);
        	Curereducer.computeClusterHashMap(k, c, alpha, Q, T, cl);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        cl=Curereducer.getClusters(Q);
        //write to the output
        int counter=1;
        CSVWriter writer=createCsvWrtier(finalOutputPath);
		for(int i=0;i<cl.size();i++) {
			List<Point> rl=cl.get(i).getRep();
			for(int j=0;j<rl.size();j++) {
				String line=counter+" "+rl.get(0);
				writer.writeNext(line.split(" "));
				
			}
			counter++;
			
		}
		closeFile(writer);
		
		
	}
	public static void closeFile(CSVWriter writer) throws IOException {
		writer.close();
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
	public static void main(String[] args) throws IOException {
		runSecondPass(args[0], args[1]);
	}

}
