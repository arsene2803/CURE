package util;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import com.opencsv.CSVWriter;


public class PointsCsv {
	public static List<String> getPoints(int numPoints,int range){
		Random rand=new Random();
		int count=0;
		HashSet<String> pointset=new HashSet<String>();
		List<String> result=new ArrayList<>();
		while(count<=numPoints){
			float x=rand.nextFloat()*range;
			float y=rand.nextFloat()*range;
			String point=x+","+y;
			if(!pointset.contains(point)){
				pointset.add(point);
				result.add(point);
				count++;
			}
			
		}
		return result;
		
	}
	public static void writeCSV(List<String> input,String fileName){
		try {
			CSVWriter writer=new CSVWriter(new FileWriter(fileName),',');
			for(int i=0;i<input.size();i++){
				String[] row=input.get(i).split(",");
				writer.writeNext(row);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void main(String[] args){
		List<String> input=getPoints(100000, 10000);
		writeCSV(input,"input.csv");
		
		
	}

}
