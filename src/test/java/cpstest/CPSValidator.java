package cpstest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import analytics.util.CPSUtil;
import analytics.util.MongoNameConstants;
import analytics.util.dao.CPOutBoxDAO;

public class CPSValidator {

	public static void main(String[] args) {		
	
			// args - input file names 
			if(args==null||args.length==0)
			System.exit(0);	
			
			String mdtagsFileName = args[0];
			String testresults=args[1];
			String env = args[2];
			String line ="";
			System.setProperty(MongoNameConstants.IS_PROD, env);
			
			FileReader fileReader = null;
			try {
				
				String outputFile = testresults + System.currentTimeMillis() + ".txt";
				File result = new File(outputFile);
				PrintWriter printWriter = null;
				printWriter = new PrintWriter(result, "UTF-8");
				fileReader = new FileReader(mdtagsFileName);
				BufferedReader buffReader = new BufferedReader(fileReader);
				while ((line = buffReader.readLine()) != null) {
					parseLine(line, printWriter);
					
				}
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				System.exit(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

	private static void parseLine(String line, PrintWriter printWriter) {
		JsonParser parser = new JsonParser();
		JsonElement jsonElement = null;
		jsonElement = parser.parse(line);
		JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("lyl_id_no");
		JsonElement mdtags = jsonElement.getAsJsonObject().get("tags");
		if(!isMemberInOutbox(lyl_id_no.getAsString()))			
		{
			printWriter.println( "lyl_id_no:"+lyl_id_no.getAsString()+", tags:" + mdtags.getAsString());
		}

	}

	private static boolean isMemberInOutbox(String lyl_id_no) {
	
		int count = new CPOutBoxDAO().getQueuedCount(lyl_id_no);
		if(count>0)
			return true;
		else
			return false;
	}
}


