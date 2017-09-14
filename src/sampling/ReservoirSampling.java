package sampling;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class ReservoirSampling {
	public static List<String[]> readCSV(String fileName){
		try {
			CSVReader reader=new CSVReader(new FileReader(fileName),',');
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
	
	public static void sampleInput(List<String[]> input,float percentage,String fileName){
		
		int k=(int) (input.size()*percentage/100),i;
		List<String[]> reservoir=new ArrayList<>();
		Random r =new Random();
		
		for(i=0;i<k;i++){
			reservoir.add(input.get(i));
		}
		
		//random sampling
		for(;i<input.size();i++){
			int j=r.nextInt(i+1);
			if(j<k)
				reservoir.set(j, input.get(i));
		}
		
		//writing to the file
		try {
			CSVWriter writer =new CSVWriter(new FileWriter(fileName),',');
			for(int l=0;l<reservoir.size();l++){
				writer.writeNext(reservoir.get(l));
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}
	
	public static void main(String[] args){
		sampleInput(readCSV(args[0]),60,args[1]);
		System.out.println("Sampling done");
	}

}
