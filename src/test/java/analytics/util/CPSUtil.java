package analytics.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;







import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;

import cpstest.CPOutBoxItem;
import analytics.util.KafkaUtil;
import analytics.util.dao.CPOutBoxDAO;

public class CPSUtil {

	public void processFile(String presetFile, String testFile, String verifyFile) {
		FileReader fileReader = null;
		String currentTopic = "stormtopic";
		String outputFile = "C:\\CPTest\\testresults_"+System.currentTimeMillis()+".txt";
		File result =new File(outputFile);
		PrintWriter printWriter = null;
		Map<String, List<CPOutBoxItem>> presetMap=loadFile(presetFile, "PRESET");
		Map<String, List<CPOutBoxItem>> testMap=loadFile(testFile, "TEST");
		Map<String, List<CPOutBoxItem>> verifyMap=loadFile(verifyFile, "VERIFY");
		
			try {
				printWriter = new PrintWriter(result, "UTF-8");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
				
		for (List<CPOutBoxItem> presetList: presetMap.values())
		{
			try {
				//PRESET

				System.out.println(presetList.size());
				String loyID=presetList.get(0).getLoy_id();
				for (CPOutBoxItem cpItem: presetList )
				{
					new CPOutBoxDAO().insertRow(cpItem);
								
				}
				// TEST
				if(!testMap.containsKey(loyID))
					break;
				List<CPOutBoxItem> testList=testMap.get(loyID);
				
				CPOutBoxItem testItem=testList.get(0);
				String kafkaMSG = createJson(testItem.getLoy_id(),
						testItem.getMdTagList());
				try {
					new KafkaUtil("PROD").sendKafkaMSGs(kafkaMSG, currentTopic);
				} catch (ConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//VERIFY
				
				if(!verifyMap.containsKey(loyID))
					break;
				//Compare the input data with the one in database to determine success or failure
				// In case of hyphen, the row should not be there in the outbox
				List<CPOutBoxItem> verifyList=verifyMap.get(loyID);
				
				for (CPOutBoxItem verifyItem: verifyList )
				{
					//new CPOutBoxDAO().insertRow(cpItem);
					verifyItem.setStatus(0);
					CPOutBoxItem queuedItem = new CPOutBoxDAO()
					.getQueuedItem(verifyItem.getLoy_id(),
							verifyItem.getMd_tag(),
							verifyItem.getStatus());
					
					String testresult = null;
					if(queuedItem!=null)
					{	if (queuedItem.getSend_date().equalsIgnoreCase(
							verifyItem.getSend_date())) {
						testresult = "success:" + verifyItem.getLoy_id()
								+ ", " + verifyItem.getMd_tag() + ", "
								+ verifyItem.getSend_date() + "";
					} else {

						testresult = "failure:" + verifyItem.getLoy_id()
								+ ", " + verifyItem.getMd_tag() + ", "
								+ verifyItem.getSend_date() + " vs queued entry :"
								+ queuedItem.getSend_date();
					}
					}
					else
					{
						testresult = "failure: The item is not queued " + verifyItem.getLoy_id()
								+ ", " + verifyItem.getMd_tag() + ", "
								+ verifyItem.getSend_date() ;
					}

					printWriter.println(testresult);
					printWriter.flush();
					
								
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			
			
			
		}
		
	}

	private Map<String,List <CPOutBoxItem>> loadFile(String filename, String testPhase) {
		FileReader fileReader = null;
		Map<String, List <CPOutBoxItem>> fileMap = new HashMap<String,List <CPOutBoxItem>>();
		String line = new String();
		try {
			fileReader = new FileReader(filename);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			System.exit(0);
		}
		BufferedReader buffReader = new BufferedReader(fileReader);	
		
		try {
			while ((line = buffReader.readLine()) != null) {
				
				CPOutBoxItem cpOutBoxItem = parseLine(line, testPhase);
				if(fileMap.containsKey(cpOutBoxItem.getLoy_id()))
				{
					List<CPOutBoxItem> existingList=fileMap.get(cpOutBoxItem.getLoy_id());
					existingList.add(cpOutBoxItem);
					
				}
				else{
					List<CPOutBoxItem> newList= new ArrayList<CPOutBoxItem>();
					newList.add(cpOutBoxItem);
				fileMap.put(cpOutBoxItem.getLoy_id(), newList);
				}
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
				
		// TODO Auto-generated method stub
		return fileMap;
	}

	private CPOutBoxItem parseLine(String line, String testPhase) {
		
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		String today = ft.format(dNow);

		CPOutBoxItem cpBoxItem = new CPOutBoxItem();
		String[] variables = line.split(",");
		if (variables.length > 0) {
			    //******  Combine all tags for the test part and send it as a single JSON object
			if("TEST".equalsIgnoreCase(testPhase))
			{
				cpBoxItem.setLoy_id(variables[0]);
				for(int i=1;i<variables.length;i++)
				{
					cpBoxItem.getMdTagList().add(variables[i]);
				}
				
			}
			else {	cpBoxItem.setLoy_id(variables[0]);
				cpBoxItem.setMd_tag(variables[1]);
			// Variable 2 need to be mapped
			if ("PRESET".equalsIgnoreCase(testPhase)) {
				// Member number,Existing Tag,Effective Date,Send Date,Sent flag
				String sendDT=null;
				if(variables.length>3)
				sendDT=getDate(Integer.parseInt(variables[3]));
				cpBoxItem.setSend_date(sendDT);
				if(variables.length>4)
				cpBoxItem.setStatus(Integer.parseInt(variables[4]));

			} else if ("VERIFY".equalsIgnoreCase(testPhase)) {
				// Member number,Incoming tags,Send Date
				if(variables.length>2)
				if(variables[2].trim().equalsIgnoreCase("-"))
					cpBoxItem.setSend_date(null);
				else{
				String sendDT=getDate(Integer.parseInt(variables[2]));
				cpBoxItem.setSend_date(sendDT);
				}

			}
			}
		}
		return cpBoxItem;
	}

	private String createJson(String lyl_id_no, List<String> tagList) {
		StringBuilder jsonBuilder = new StringBuilder();

		jsonBuilder.append("{\"lyl_id_no\":\"").append(lyl_id_no)
				.append("\",\"tags\":[");
		if (!tagList.isEmpty()) {
			boolean firstTag = true;
			for (String tag : tagList) {
				if (firstTag) {
					firstTag = false;
					jsonBuilder.append("\"").append(tag).append("\"");
				} else {
					jsonBuilder.append(",\"").append(tag).append("\"");
				}
			}
		}
		jsonBuilder.append("]}");

		return jsonBuilder.toString();
	}

	
	private String getDate(int numofdays)
	{
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, numofdays); 
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd"); 
		return s.format(new Date(cal.getTimeInMillis()));
	}
	
	
}
