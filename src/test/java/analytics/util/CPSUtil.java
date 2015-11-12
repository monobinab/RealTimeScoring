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
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import cpstest.CPOutBoxItem;
import analytics.util.KafkaUtil;	
import analytics.util.dao.CPOutBoxDAO;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.MemberScoreDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.EmailPackage;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.ModelScore;
import analytics.util.objects.TagMetadata;

public class CPSUtil {

	public void processFile(String presetFile, String testFile, String verifyFile,
							String outputfile, String topicName) {

		String outputFile = outputfile + System.currentTimeMillis() + ".txt";
		File result = new File(outputFile);
		PrintWriter printWriter = null;
		int successCount = 0;
		int failureCount = 0;
		String modPercentile = null;
		MemberMDTags2Dao memberMDTags2Dao = new MemberMDTags2Dao();
		CpsOccasionsDao cpsOccasion;
		HashMap<String, String> cpsOccasionPriorityMap;
		HashMap<String, String> cpsOccasionDurationMap;
		HashMap<String, String> cpsOccasionsByIdMap;
		cpsOccasion = new CpsOccasionsDao();
		cpsOccasionPriorityMap = cpsOccasion.getcpsOccasionPriority();
		cpsOccasionDurationMap = cpsOccasion.getcpsOccasionDurations();
		cpsOccasionsByIdMap = cpsOccasion.getcpsOccasionsById();
		ChangedMemberScoresDao changedMemberScoresDao = new ChangedMemberScoresDao();
		TagVariableDao tagVariableDao = new TagVariableDao();
		MemberScoreDao memberScoreDao = new MemberScoreDao();
		TagMetadataDao tagMetadataDao = new TagMetadataDao();
		MemberInfoDao memberInfoDao = new MemberInfoDao();
		ModelPercentileDao modelPercentileDao = new ModelPercentileDao();
		
		Map<String, List<CPOutBoxItem>> presetMap = loadFile(presetFile, "PRESET");
		Map<String, List<CPOutBoxItem>> testMap = loadFile(testFile, "TEST");
		Map<String, List<CPOutBoxItem>> verifyMap = loadFile(verifyFile, "VERIFY");

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
				String l_id = SecurityUtils.hashLoyaltyId(loyID);
				
				Long membRecCount  = memberInfoDao.getMemberInfoCount(l_id);
				if(membRecCount == 0){
					//add memberInfo
					memberInfoDao.addMemberInfo(l_id);					 
				}
				else if(membRecCount > 1){
					memberInfoDao.deleteMemberInfo(l_id);
					memberInfoDao.addMemberInfo(l_id);	
				}
				changedMemberScoresDao.deleteChangedMemberScore(l_id);
				//delete the documents if more than 1 document exist for this lid.
				//get defaultMember scores and add to this member scores.
				Long membScoreRecCount = memberScoreDao.getMemberInfoCount(l_id);
				if(membScoreRecCount >1)
					memberScoreDao.deleteMemberScore(l_id);
				Map<String,Double> defaultMemberScores = memberScoreDao.getMemberScores2("defaultMember");
				memberScoreDao.upsertUpdateMemberScores(l_id, defaultMemberScores);
				//}
				CPOutBoxItem presetItem = presetList.get(0);

				printWriter.println("PRESET OutBox Entries for LOYALTY ID: "+ loyID);

				for (CPOutBoxItem cpItem : presetList) {
					
					TagMetadata tagMetadata = tagMetadataDao.getDetails(cpItem.getMd_tag());
					
					if(tagMetadata==null){
						tagMetadata = new TagMetadata();
						tagMetadata.setMdTag(cpItem.getMd_tag());
						tagMetadata.setPurchaseOccassion(cpsOccasionsByIdMap.get(cpItem.getMd_tag().substring(5,6)));		
						tagMetadata.setBusinessUnit("testBu");
						tagMetadata.setSubBusinessUnit("testSubBu");
						tagMetadataDao.addTagMetaData(tagMetadata);						
					}
					
					cpItem.setBu(tagMetadata.getBusinessUnit());
					cpItem.setSub_bu(tagMetadata.getSubBusinessUnit());
					cpItem.setOccasion_name(tagMetadata.getPurchaseOccasion());
								
					new CPOutBoxDAO().insertRow(cpItem);

					printWriter.println("tag:" + cpItem.getMd_tag()
							+ "  Send Date:" + cpItem.getSend_date()
							+ " Sent Flag:" + cpItem.getStatus());
					
					presetItem.getMdTagList().add(cpItem.getMd_tag());
				}
				//This is to make sure preset data is populated in membermdTags with Dates.
				//For purchases and purchase Top5 preset data will not be available in membermdTags with Dates if we don't do this.
				List<String> mdTags = memberMDTags2Dao.getMemberMDTags(l_id);
				if(mdTags!=null && mdTags.size()>0){
					memberMDTags2Dao.deleteMemberMDTags(l_id);
				}
				memberMDTags2Dao.addMemberMDTags(l_id, presetItem.getMdTagList(), cpsOccasionDurationMap, cpsOccasionPriorityMap);
				

				SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
				// TEST
				// if(!testMap.containsKey(loyID))
				// break;
				if (testMap.containsKey(loyID)) {
					l_id = SecurityUtils.hashLoyaltyId(loyID);
					List<CPOutBoxItem> testList = testMap.get(loyID);
					CPOutBoxItem testItem = testList.get(0);
				
					for (CPOutBoxItem cptestItem : testList) {
						
						//update the score of the model pertaining to the purchase to 0
						// in changedMemberScore/memberScore to 0 if testList has a purchase tag 
						
						if(cptestItem.getMd_tag().contains("Purchase")){
									
							ModelScore modelScorePercentile1 = new ModelScore();
							Double newScore = 0.0;
							Integer modelId = tagVariableDao.getmodelIdFromTag(cptestItem.getMd_tag().substring(0,5));
							//Black out this model
							if(modelId !=null){
								Map<Integer, ChangedMemberScore>  changedScores = changedMemberScoresDao.getChangedMemberScores(l_id,modelId);
								if(changedScores!=null && changedScores.size()>0){
									ChangedMemberScore changedMemberScoreObject = changedScores.get(Integer.toString(modelId));
									if(changedMemberScoreObject==null){
										changedMemberScoreObject = new ChangedMemberScore();
									}
									changedMemberScoreObject.setModelId(Integer.toString(modelId));
									changedMemberScoreObject.setScore(newScore );
									changedMemberScoreObject.setEffDate(sdformat.format(new Date()));
									changedMemberScoreObject.setMaxDate(sdformat.format(new Date()));
									changedMemberScoreObject.setMinDate(sdformat.format(new Date()));
									changedScores.put(modelId, changedMemberScoreObject);	
									changedMemberScoresDao.upsertUpdateChangedScores(l_id, changedScores);
								}
								else{
									Map<String, Double> memScores = memberScoreDao.getMemberScores2(l_id);
									//Map<String, Double> memScores = memberScoreDao.getMemberScores(l_id, modelId);
									if(memScores!=null && memScores.size()>0){
										memScores.put(Integer.toString(modelId), newScore);
										memberScoreDao.upsertUpdateMemberScores(l_id, memScores);	
									}
								}
								modelScorePercentile1.setModelId(Integer.toString(modelId));
								modelScorePercentile1.setScore(newScore);	
								modelScorePercentile1.setPercentile(1);
								testItem.getModelScorePercentiles().add(modelScorePercentile1);
								//modelScorePercentile.setPercentile(scoringUtils.getPercentileForScore(newScore,modelId));
								
							}
							
							continue;
						}
						
						//update the score of the model pertaining to top5 to maxscore of 98th percentile of that model
						//in changedMemberScore/memberScore
						if(cptestItem.getMd_tag().contains("Top5")){
							ModelScore modelScorePercentile2 = new ModelScore();
							double newScore = 0.0;
							Integer modelId = tagVariableDao.getmodelIdFromTag(cptestItem.getMd_tag().substring(0,5));
							//reset the score to be max(score) of 98th percentile for that model.
							if(modelId !=null){
								HashMap <String,String> modelPercentile = new HashMap<String, String>();
								modelPercentile = modelPercentileDao.getModelWith98Percentile();
								if(modelPercentile!=null && modelPercentile.size()>0){
									modPercentile = modelPercentile.get(Integer.toString(modelId));
									if(StringUtils.isNotEmpty(modPercentile))
										newScore = Double.parseDouble(modPercentile);
									//check if there is an entry for this member in changedMemberScores
									//if so update it
									Map<Integer, ChangedMemberScore>  changedScores = changedMemberScoresDao.getChangedMemberScores(l_id,modelId);
									if(changedScores!=null && changedScores.size()>0){
										ChangedMemberScore changedMemberScoreObject = changedScores.get(Integer.toString(modelId));
										if(changedMemberScoreObject==null){
											changedMemberScoreObject = new ChangedMemberScore();
										}
										changedMemberScoreObject.setModelId(Integer.toString(modelId));
										changedMemberScoreObject.setScore(newScore );
										changedMemberScoreObject.setEffDate(sdformat.format(new Date()));
										changedMemberScoreObject.setMaxDate(sdformat.format(new Date()));
										changedMemberScoreObject.setMinDate(sdformat.format(new Date()));
										changedScores.put(modelId, changedMemberScoreObject);									
										changedMemberScoresDao.upsertUpdateChangedScores(l_id, changedScores);
									}
									else{
										//check if there is an entry for this member in memberScores
										//if so update it								
										//Map<String, Double> memScores = memberScoreDao.getMemberScores(l_id, modelId);
										Map<String, Double> memScores = memberScoreDao.getMemberScores2(l_id);
										if(memScores!=null && memScores.size()>0){
										//	Map<String, String> memberScores = new HashMap<String, String>();
											memScores.put(Integer.toString(modelId),newScore);
											memberScoreDao.upsertUpdateMemberScores(l_id, memScores);
										}
									}
									modelScorePercentile2.setModelId(Integer.toString(modelId));
									modelScorePercentile2.setScore(newScore);
									modelScorePercentile2.setPercentile(98);
									//modelScorePercentile.setPercentile(scoringUtils.getPercentileForScore(newScore,modelId));
									testItem.getModelScorePercentiles().add(modelScorePercentile2);
									
								}								
								
							}
							
							continue;
						}
						if(StringUtils.isNotBlank(cptestItem.getMd_tag())){
							TagMetadata tagMetadata = tagMetadataDao.getDetails(cptestItem.getMd_tag());
							
							if(tagMetadata==null){
								tagMetadata = new TagMetadata();
								tagMetadata.setMdTag(cptestItem.getMd_tag());
								tagMetadata.setPurchaseOccassion(cpsOccasionsByIdMap.get(cptestItem.getMd_tag().substring(5, 6)));		
								tagMetadata.setBusinessUnit("testBu");
								tagMetadata.setSubBusinessUnit("testSubBu");
								tagMetadataDao.addTagMetaData(tagMetadata);						
							}
						}
						testItem.getMdTagList().add(cptestItem.getMd_tag());
					}
					

					try {
						if(testItem.getMdTagList().size()>0){

							String kafkaMSG = createJson(testItem.getLoy_id(),testItem.getMdTagList());
							System.out.println("message sent to kafka: "+ kafkaMSG);
							new KafkaUtil("PROD")
							.sendKafkaMSGs(kafkaMSG, topicName);
						}
						if(testItem.getModelScorePercentiles().size()>0){
							String kafkaMSG = createModelScorePercentileJson(testItem.getLoy_id(),testItem.getModelScorePercentiles());
							System.out.println("message sent to kafka: "+ kafkaMSG);
							new KafkaUtil("PROD")
							.sendKafkaMSGs(kafkaMSG, "rts_cp_purchase_scores_qa");
						}
						
							
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
						
						CPOutBoxItem queuedItem = null;
						/*if(verifyItem.getMd_tag().contains("Purchase")){
							queuedItem = new CPOutBoxDAO()
							.getQueuedItem(verifyItem.getLoy_id(),
									verifyItem.getMd_tag().substring(0,5),
									verifyItem.getStatus());
						}
						
						else */
							queuedItem = new CPOutBoxDAO()
								.getQueuedItem(verifyItem.getLoy_id(),
										verifyItem.getMd_tag(),
										verifyItem.getStatus());

						String testresult = null;
						
					/*	if(verifyItem.getMd_tag().contains("Purchase")){
							if(queuedItem!= null  && queuedItem.getSend_date() != null ){
								if(!verifyItem.getMd_tag().substring(0,5).equalsIgnoreCase(queuedItem.getMd_tag().substring(0,5))){
									successCount++;
									testresult = "SUCCESS TestTag: "
											+ verifyItem.getMd_tag()
											+ " is not affecting the queued tag " +queuedItem.getMd_tag();
								}
								else{
									failureCount++;
									testresult = "FAILURE Test tag: "
										+ verifyItem.getMd_tag()
										+ " is Queued in OutBox which is not expected" ;
								}

								
							}
							else {
								successCount++;
								testresult = "SUCCESS TestTag: "
										+ verifyItem.getMd_tag()
										+ " is not queued to OutBox as Expected";
								if(!verifyItem.getMd_tag().substring(0,5).equalsIgnoreCase(queuedItem.getMd_tag().substring(0,5))){
									successCount++;
									testresult = "SUCCESS TestTag: "
											+ verifyItem.getMd_tag()
											+ " is not affecting the queued tag " +queuedItem.getMd_tag();
								}
								else{
									failureCount++;
									testresult = "FAILURE Test tag: "
										+ verifyItem.getMd_tag()
										+ " is Queued in OutBox which is not expected" ;
								}
							}
						}

						// Printing the test result file
						else*/
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
				if(variables.length>1){
					cpBoxItem.setMd_tag(variables[1]);
				}else{
					cpBoxItem.setMd_tag(StringUtils.EMPTY);
				}
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
				.append("\"");
		if (!tagList.isEmpty()) {
			boolean firstTag = true;
			jsonBuilder.append(",\"tags\":[");
			for (String tag : tagList) {
				if (firstTag) {
					firstTag = false;
					jsonBuilder.append("\"").append(tag).append("\"");
				} else {
					jsonBuilder.append(",\"").append(tag).append("\"");
				}
			}
			jsonBuilder.append("]");
		}
		jsonBuilder.append("}");

		return jsonBuilder.toString();
	}
	
	private String createModelScorePercentileJson(String lyl_id_no, List<ModelScore> modelScorePercentiles) {
		TagVariableDao tagVariableDao = new TagVariableDao();
		Set<Integer>models = tagVariableDao.getModels();
		JSONObject mainJsonObj = new JSONObject();
		mainJsonObj.put("memberId", lyl_id_no);
		JSONArray jsonArray = new JSONArray();
		if(modelScorePercentiles!=null && modelScorePercentiles.size()>0){
			for(ModelScore modelScorePercentile : modelScorePercentiles){
				if(models.contains(Integer.parseInt(modelScorePercentile.getModelId()))){
					JSONObject jsonObj = new JSONObject();
					jsonObj.put("modelId", modelScorePercentile.getModelId());
					jsonObj.put("score", modelScorePercentile.getScore());
					jsonObj.put("percentile",modelScorePercentile.getPercentile() );
					jsonArray.add(jsonObj);				
				}				
			}
			mainJsonObj.put("scoresInfo", jsonArray);			
		}		
		return mainJsonObj.toJSONString();
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
