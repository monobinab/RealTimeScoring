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

	public void processFile(String presetFile, String testFile, String outputfile) {
		FileReader fileReader = null;
		String currentTopic = "stormtopic";
		String outputFile = outputfile+System.currentTimeMillis()+".txt";
		File result =new File(outputFile);
		PrintWriter printWriter = null;
		Map<String, List<CPOutBoxItem>> presetMap=loadFile(presetFile, "PRESET");
		Map<String, List<CPOutBoxItem>> testMap=loadFile(testFile, "TEST");
		Map<String, List<CPOutBoxItem>> verifyMap=loadFile(testFile, "VERIFY");
		int successCount=0;
		int failureCount=0;
		
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
				StringBuffer presetBuffer=new StringBuffer();
				printWriter.println("PRESET OutBox Entries for LOYALTY ID: "+loyID);
				//presetBuffer.append("The Preset values are given below for Loyalty ID: "+loyID+"\n");
				for (CPOutBoxItem cpItem: presetList )
				{
					new CPOutBoxDAO().insertRow(cpItem);
					//printWriter.println("tag:"+cpItem.getMd_tag()+"Added Date:"+cpItem.getAdded_datetime()+"  Send Date:"+cpItem.getSend_date() +" Sent Flag:"+cpItem.getStatus() );
					printWriter.println("tag:"+cpItem.getMd_tag()+"  Send Date:"+cpItem.getSend_date() +" Sent Flag:"+cpItem.getStatus() );
					//presetBuffer.append("tag:"+cpItem.getMd_tag()+"  Send Date:"+cpItem.getSend_date() +" Sent Flag:"+cpItem.getStatus()+"/n" );
								
				}
				//printWriter.println(presetBuffer);
				// TEST
//				if(!testMap.containsKey(loyID))
//					break;
				if(testMap.containsKey(loyID))
				{
				List<CPOutBoxItem> testList=testMap.get(loyID);
				CPOutBoxItem testItem=testList.get(0);
				for (CPOutBoxItem cptestItem: testList )
				{
					testItem.getMdTagList().add(cptestItem.getMd_tag());
								
				}
								
				String kafkaMSG = createJson(testItem.getLoy_id(),
						testItem.getMdTagList());
				
				try {
					new KafkaUtil("PROD").sendKafkaMSGs(kafkaMSG, currentTopic);
				} catch (ConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				Thread.sleep(5000);
				
				//VERIFY
				
//				if(!verifyMap.containsKey(loyID))
//					break;
				//Compare the input data with the one in database to determine success or failure
				// In case of hyphen, the row should not be there in the outbox
				List<CPOutBoxItem> verifyList=verifyMap.get(loyID);
				printWriter.println("TEST RESULTS");
				for (CPOutBoxItem verifyItem: verifyList )
				{
					//new CPOutBoxDAO().insertRow(cpItem);
					verifyItem.setStatus(0);
					CPOutBoxItem queuedItem = new CPOutBoxDAO()
					.getQueuedItem(verifyItem.getLoy_id(),
							verifyItem.getMd_tag(),
							verifyItem.getStatus());
					
					String testresult = null;
					
					// Printing the test file 
					
					
					if(queuedItem!=null&&queuedItem.getSend_date()!=null)
					{							
						if(queuedItem.getSend_date().equalsIgnoreCase(
							verifyItem.getSend_date())) {
							successCount++;
						testresult = "SUCCESS Test Tag: " + verifyItem.getMd_tag()
								+ " OutBox Send Date " + queuedItem.getSend_date() + " is SAME as Expected Send Date: "
								+ verifyItem.getSend_date();
					} else {
						failureCount++;
						if(verifyItem.getSend_date()==null)
						{
							testresult = "FAILURE TestTag: " + verifyItem.getMd_tag()
									+ " is NOT removed from OutBox as Expected";
						}
						else
							{testresult = "FAILURE Test Tag: " + verifyItem.getMd_tag()
								+ " OutBox Send Date " + queuedItem.getSend_date() + " is NOT SAME as Expected Send Date: "
								+ verifyItem.getSend_date();
							}
					}
					}
					else
					{
						if(verifyItem.getSend_date()==null)
						{
							
							successCount++;
							testresult = "SUCCESS TestTag: " + verifyItem.getMd_tag()
									+ " is removed from OutBox as Expected";
									
						}
						else{
							failureCount++;
						testresult = "FAILURE Test tag: " + verifyItem.getMd_tag()
								+ " is NOT Queued in OutBox with Expected Send Date : " 	+ verifyItem.getSend_date();
						}
					}

					
					printWriter.println(testresult);		
					
								
				}
				printWriter.println();

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			

		}
		
		printWriter.println("TOTAL NUMBER OF SUCESSFULL TESTS :"+successCount);
		printWriter.println("TOTAL NUMBER OF FAILED TESTS :     "+failureCount);
		printWriter.flush();
		
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
				cpBoxItem.setMd_tag(variables[1]);
//				for(int i=1;i<variables.length;i++)
//				{
//					cpBoxItem.getMdTagList().add(variables[i]);
//				}
//				
			}
			else {	cpBoxItem.setLoy_id(variables[0]);
				cpBoxItem.setMd_tag(variables[1]);
			// Variable 2 need to be mapped
			if ("PRESET".equalsIgnoreCase(testPhase)) {
				// Member number,Existing Tag,Effective Date,Send Date,Sent flag
				String sendDT=null;
//				Date addedDate=getDate(Integer.parseInt(variables[2]));
//				cpBoxItem.setAdded_datetime(addedDate);
				if(variables.length>3)
				{sendDT=getDateString(Integer.parseInt(variables[3]));
				cpBoxItem.setSend_date(sendDT);
				}
				if(variables.length>4)
				cpBoxItem.setStatus(Integer.parseInt(variables[4]));

			} else if ("VERIFY".equalsIgnoreCase(testPhase)) {
				// Member number,Incoming tags,Send Date
				if(variables.length>2)
				if(variables[2].trim().equalsIgnoreCase("-"))
					cpBoxItem.setSend_date(null);
				else{
				String sendDT=getDateString(Integer.parseInt(variables[2]));
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

	
	private String getDateString(int numofdays)
	{
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, numofdays); 
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd"); 
		return s.format(new Date(cal.getTimeInMillis()));
	}
	
	private Date getDate(int numofdays)
	{
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, numofdays); 
		
		return new Date(cal.getTimeInMillis());
	}
	
	
}
