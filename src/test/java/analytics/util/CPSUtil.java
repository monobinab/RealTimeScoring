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
import org.apache.commons.lang.StringUtils;

import cpstest.CPOutBoxItem;
import analytics.util.KafkaUtil;
import analytics.util.dao.CPOutBoxDAO;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.TagMetadata;

public class CPSUtil {

	public void processFile(String presetFile, String testFile,
							String outputfile, String topicName) {

		String outputFile = outputfile + System.currentTimeMillis() + ".txt";
		File result = new File(outputFile);
		PrintWriter printWriter = null;
		int successCount = 0;
		int failureCount = 0;
		
		Map<String, List<CPOutBoxItem>> presetMap = loadFile(presetFile, "PRESET");
		Map<String, List<CPOutBoxItem>> testMap = loadFile(testFile, "TEST");
		Map<String, List<CPOutBoxItem>> verifyMap = loadFile(testFile, "VERIFY");

		try {
			printWriter = new PrintWriter(result, "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		for (List<CPOutBoxItem> presetList : presetMap.values()) {
			try {
				// PRESET
				String loyID = presetList.get(0).getLoy_id();

				printWriter.println("PRESET OutBox Entries for LOYALTY ID: "+ loyID);

				for (CPOutBoxItem cpItem : presetList) {
					
					TagMetadata tagMetadata = new TagMetadataDao().getDetails(cpItem.getMd_tag());
					//ToDO - if metadata is not found intagMetadata collection populate the 
					//       tagMetadata object by getting the metadata from Teradata
					if(tagMetadata==null){
						System.out.println("metadata is not found for tag: "+cpItem.getMd_tag() );
						// ToDO - populate the tagMetadata object by getting the metadata from Teradata
					}
					if(StringUtils.isBlank(cpItem.getBu()))
						cpItem.setBu(tagMetadata.getBusinessUnit());
					if(StringUtils.isBlank(cpItem.getSub_bu()))
						cpItem.setSub_bu(tagMetadata.getSubBusinessUnit());
					if(StringUtils.isBlank(cpItem.getOccasion_name()))
						cpItem.setOccasion_name(tagMetadata.getPurchaseOccasion());
								new CPOutBoxDAO().insertRow(cpItem);

					printWriter.println("tag:" + cpItem.getMd_tag()
							+ "  Send Date:" + cpItem.getSend_date()
							+ " Sent Flag:" + cpItem.getStatus());
				}

				// TEST
				// if(!testMap.containsKey(loyID))
				// break;
				if (testMap.containsKey(loyID)) {
					List<CPOutBoxItem> testList = testMap.get(loyID);
					CPOutBoxItem testItem = testList.get(0);
					ChangedMemberScoresDao changedMemberScoresDao = new ChangedMemberScoresDao();
					TagVariableDao tagVariableDao = new TagVariableDao();
					for (CPOutBoxItem cptestItem : testList) {
						
						if(cptestItem.getMd_tag().contains("Purchase")){
							Integer modelId = tagVariableDao.getmodelIdFromTag(cptestItem.getMd_tag().substring(0,5));
							Map<Integer, ChangedMemberScore>  changedScores = changedMemberScoresDao.getChangedMemberScores(loyID,modelId);
							changedMemberScoresDao.upsertUpdateChangedScores(loyID, changedScores);
							continue;
						}

						testItem.getMdTagList().add(cptestItem.getMd_tag());
					}

					String kafkaMSG = createJson(testItem.getLoy_id(),testItem.getMdTagList());
					System.out.println("message sent to kafka: "+ kafkaMSG);

					try {
						new KafkaUtil("PROD")
								.sendKafkaMSGs(kafkaMSG, topicName);
					} catch (ConfigurationException e) {						
						e.printStackTrace();
					}

					Thread.sleep(5000);

					// VERIFY

					// if(!verifyMap.containsKey(loyID))
					// break;
					// Compare the input data with the one in database to
					// determine success or failure
					// In case of hyphen, the row should not be there in the
					// outbox
					List<CPOutBoxItem> verifyList = verifyMap.get(loyID);
					printWriter.println("TEST RESULTS");
					for (CPOutBoxItem verifyItem : verifyList) {
						verifyItem.setStatus(0);
						CPOutBoxItem queuedItem = new CPOutBoxDAO()
								.getQueuedItem(verifyItem.getLoy_id(),
										verifyItem.getMd_tag(),
										verifyItem.getStatus());

						String testresult = null;

						// Printing the test result file
						if (queuedItem != null && queuedItem.getSend_date() != null) {
							if (queuedItem.getSend_date().equals(verifyItem.getSend_date()))   {
								successCount++;
								testresult = "SUCCESS Test Tag: "
										+ verifyItem.getMd_tag()
										+ " OutBox Send Date "
										+ queuedItem.getSend_date()
										+ " is SAME as Expected Send Date: "
										+ verifyItem.getSend_date();
							} else {
								failureCount++;
								if (verifyItem.getSend_date() == null) {
									testresult = "FAILURE TestTag: "
											+ verifyItem.getMd_tag()
											+ " is NOT Expected to be queued";
								} else {
									testresult = "FAILURE Test Tag: "
											+ verifyItem.getMd_tag()
											+ " OutBox Send Date "
											+ queuedItem.getSend_date()
											+ " is NOT SAME as Expected Send Date: "
											+ verifyItem.getSend_date();
								}
							}
						} else {
							if (verifyItem.getSend_date() == null) {

								successCount++;
								testresult = "SUCCESS TestTag: "
										+ verifyItem.getMd_tag()
										+ " is not queued to OutBox as Expected";

							} else {
								failureCount++;
								testresult = "FAILURE Test tag: "
										+ verifyItem.getMd_tag()
										+ " is NOT Queued in OutBox with Expected Send Date : "
										+ verifyItem.getSend_date();
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

		printWriter.println("TOTAL NUMBER OF SUCESSFULL TESTS :" + successCount);
		printWriter.println("TOTAL NUMBER OF FAILED TESTS     :"+ failureCount);
		printWriter.flush();

	}

	private Map<String, List<CPOutBoxItem>> loadFile(String filename,
			String testPhase) {
		FileReader fileReader = null;
		Map<String, List<CPOutBoxItem>> fileMap = new HashMap<String, List<CPOutBoxItem>>();
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
				if (fileMap.containsKey(cpOutBoxItem.getLoy_id())) {
					List<CPOutBoxItem> existingList = fileMap.get(cpOutBoxItem.getLoy_id());
					existingList.add(cpOutBoxItem);

				} else {
					List<CPOutBoxItem> newList = new ArrayList<CPOutBoxItem>();
					newList.add(cpOutBoxItem);
					fileMap.put(cpOutBoxItem.getLoy_id(), newList);
				}

			}
			buffReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// TODO Auto-generated method stub
		return fileMap;
	}

	private CPOutBoxItem parseLine(String line, String testPhase) {
		CPOutBoxItem cpBoxItem = new CPOutBoxItem();
		String[] variables = line.split(",");
		if (variables.length > 0) {
			// Combine all tags for the test part and send it as a single
			// JSON object
			if ("TEST".equalsIgnoreCase(testPhase)) {
				cpBoxItem.setLoy_id(variables[0]);
				cpBoxItem.setMd_tag(variables[1]);
			} else {
				cpBoxItem.setLoy_id(variables[0]);
				cpBoxItem.setMd_tag(variables[1]);
				// Variable 2 need to be mapped
				if ("PRESET".equalsIgnoreCase(testPhase)) {
					// Member number,Existing Tag,Added Date,Send Date,Sent flag
					String sendDT = null;
					 Date sentDateTime=null;
					String addedDate=getTimeStampString(Integer.parseInt(variables[2]));
					cpBoxItem.setAdded_datetime(addedDate);
					if (variables.length > 3) {
						sendDT = getDateString(Integer.parseInt(variables[3]));
						cpBoxItem.setSend_date(sendDT);
						
					}
					if (variables.length > 4)
						cpBoxItem.setStatus(Integer.parseInt(variables[4]));
					    if(cpBoxItem.getStatus()==1)
						{
					    	sentDateTime=getDate(Integer.parseInt(variables[3]));
					    	cpBoxItem.setSent_datetime(sentDateTime);
						}
				

				} else if ("VERIFY".equalsIgnoreCase(testPhase)) {
					// Member number,Incoming tags,Send Date
					if (variables.length > 2)
						if (variables[2].trim().equalsIgnoreCase("-"))
							cpBoxItem.setSend_date(null);
						else {
							String sendDT = getDateString(Integer
									.parseInt(variables[2]));
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

	private String getDateString(int numofdays) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, numofdays);
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
		return s.format(new Date(cal.getTimeInMillis()));
	}
	
	private String getTimeStampString(int numofdays) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, numofdays);
		SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return s.format(new Date(cal.getTimeInMillis()));
	}

	private Date getDate(int numofdays) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, numofdays);

		return new Date(cal.getTimeInMillis());
	}

}
