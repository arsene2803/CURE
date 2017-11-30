package util;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import com.opencsv.CSVWriter;


public class PointsCsv {

		
	public static void writeCSV(String fileName,int numPoints,int range){
		try {
			CSVWriter writer=new CSVWriter(new FileWriter(fileName),',');
			Random rand=new Random();
			int count=0;
			HashSet<String> pointset=new HashSet<String>();
			while(count<=numPoints){
				float x=rand.nextFloat()*range;
				float y=rand.nextFloat()*range;
				String point=x+","+y;
				if(!pointset.contains(point)){
					writer.writeNext(point.split(","));
					count++;
				}
				
			}
			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args){
		writeCSV("input.csv",100000000/2,100000);
		
		
	}

}
