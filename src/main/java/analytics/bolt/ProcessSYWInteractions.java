package analytics.bolt;

import analytics.util.SywApiCalls;
import analytics.util.dao.BoostDao;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.DivLnBoostDao;
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.MemberScoreDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.ModelSywBoostDao;
import analytics.util.dao.PidDivLnDao;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.DivLn;
import analytics.util.objects.SYWEntity;
import analytics.util.objects.SYWInteraction;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ProcessSYWInteractions extends EnvironmentBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ProcessSYWInteractions.class);
	private List<String> entityTypes;
	private OutputCollector outputCollector;
	private PidDivLnDao pidDivLnDao;
	private DivLnBoostDao divLnBoostDao;
	SywApiCalls sywApiCalls;
	private BoostDao boostDao;
	private MemberBoostsDao memberBoostsDao;
	private MemberScoreDao memberScoreDao;
	private ChangedMemberScoresDao changedMemberScoresDao;
	private ModelPercentileDao modelPercentileDao;
	private ModelSywBoostDao modelBoostDao;
	private Map<String, List<String>> boostListMap;
	
	 public ProcessSYWInteractions(String systemProperty){
		 super(systemProperty);
		 }

	 @Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		 super.prepare(stormConf, context, collector);
	     this.outputCollector = collector;
		sywApiCalls = new SywApiCalls();

		pidDivLnDao = new PidDivLnDao();
		divLnBoostDao = new DivLnBoostDao();
		boostDao = new BoostDao();
		memberBoostsDao = new MemberBoostsDao();
		memberScoreDao = new MemberScoreDao();
		changedMemberScoresDao = new ChangedMemberScoresDao();
		modelPercentileDao = new ModelPercentileDao();
		modelBoostDao = new ModelSywBoostDao();
		entityTypes = new ArrayList<String>();
		entityTypes.add("Product");
		List<String> feeds = new ArrayList<String>();
		feeds.add("SYW_LIKE");
		feeds.add("SYW_OWN");
		feeds.add("SYW_WANT");
		boostListMap = boostDao.getBoostsMap(feeds);// Feed prefix
	}

	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		String feedType = null;
		
		// Get l_id", "message", "InteractionType" from parsing bolt
		String lId = input.getStringByField("l_id");
		JsonParser parser = new JsonParser();
		JsonObject interactionObject = parser.parse(input.getStringByField("message")).getAsJsonObject();
		
		String lyl_id_no = input.getStringByField("lyl_id_no");
		

		Gson gson = new Gson();
		SYWInteraction obj = gson.fromJson(interactionObject, SYWInteraction.class);


		/* Ignore interactions that we dont want. 	
		   We currently process Catalogs: Like, Want, Own Others: Like
		 */

		// Find type of catalog to process, and hence the boost variable
		for (SYWEntity currentEntity : obj.getEntities()) {
			if (currentEntity != null) {
				//TODO: confirm with devika, catalog only has likes, product has all of them
				if ("Catalog".equals(currentEntity.getType())) {
					//interactionType
					String catalogType = sywApiCalls.getCatalogType(currentEntity.getId());
					LOGGER.debug(catalogType);
					if (catalogType != null) {
						if (catalogType.equals("Own")){
							feedType = "SYW_OWN";
						}
						else if (catalogType.equals("Like")){
							feedType = "SYW_LIKE";
						}
						else if (catalogType.equals("Want")){
							feedType = "SYW_WANT";
						}
					}
				}
			}
		}

		if (feedType == null) {
			LOGGER.info("We process only own, like and want catalogs & SYW Likes");
			redisCountIncr("customer_catalog");
			outputCollector.ack(input);  
			return;
		}
		
		Map<String, List<String>> divLnBoostVariblesMap = divLnBoostDao.getDivLnBoost();
		// Variable map stores the vars to send to Strategy Bolt
		Map<String, String> variableValueMap = new HashMap<String, String>();
		Map<String, List<String>> boostValuesMap = new HashMap<String, List<String>>();
		if (obj != null && obj.getEntities() != null) {

			for (SYWEntity currentEntity : obj.getEntities()) {
				if (currentEntity != null)
					LOGGER.debug(currentEntity.getType());
				// TODO: If more types handle in a more robust manner. If we
				// expect only Products, this makes sense
				if (currentEntity != null && "Product".equals(currentEntity.getType())) {
					String productId = sywApiCalls.getCatalogId(currentEntity.getId());
					/* Product does not exist? */
					if (productId == null) {
						LOGGER.info("Unable to get product id for " + currentEntity.getId());
						// Get the next entity
						continue;
					} else {
						// TODO: We should also handle Kmart items - in which
						// case it would be divLnItm???
						DivLn divLnObj = pidDivLnDao.getDivLnFromPid(productId);
						LOGGER.trace(" div line for " + productId + " are " + divLnObj + " lid: " + lId);
						if (divLnObj != null) {
							String div = divLnObj.getDiv();
							String divLn = divLnObj.getDivLn();
							// Get the boost variable
							if (divLnBoostVariblesMap.containsKey(div)) {
								for (String b : divLnBoostVariblesMap.get(div)) {
									if (boostListMap.get(feedType).contains(b)) {// If
																					// it
																					// belongs
																					// to
																					// current
																					// feed
										if (!boostValuesMap.containsKey(b)) {
											boostValuesMap.put(b, new ArrayList<String>());
										}
										boostValuesMap.get(b).add(productId);
									}
								}
							}
							if (divLnBoostVariblesMap.containsKey(divLn)) {
								for (String b : divLnBoostVariblesMap.get(divLn)) {
									if (boostListMap.get(feedType).contains(b)) {// If
																					// it
																					// belongs
																					// to
																					// current
																					// feed
										if (!boostValuesMap.containsKey(b)) {
											boostValuesMap.put(b, new ArrayList<String>());
										}
										boostValuesMap.get(b).add(productId);
									}
								}
							}
						}
						// Eg - BOOST_SYW_APP_LIKETCOUNT
						// divLnBoost -
						// d:004 , b:BOOST_SYW_APP_LIKETCOUNT
						// 004
						LOGGER.trace(" boost value map :" + boostValuesMap + " lid: " + lId);
					}

				}

			}
		}
		// Get the memberBoostVariables for current lid. Consolidate into a map
		Map<String, Map<String, List<String>>> allBoostValuesMap = memberBoostsDao.getMemberBoostsValues(lId, boostValuesMap.keySet());
		if (allBoostValuesMap == null) {
			allBoostValuesMap = new HashMap<String, Map<String, List<String>>>();
		}
		
		for (String b : boostValuesMap.keySet()) {
			if (allBoostValuesMap.containsKey(b)) {
				allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
			} else {
				allBoostValuesMap.put(b, new HashMap<String, List<String>>());
				allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
			}
			variableValueMap.put(b, createJsonDoc(allBoostValuesMap.get(b)));
		}
		if (variableValueMap != null && !variableValueMap.isEmpty()) {
			Type varValueType = new TypeToken<Map<String, String>>() {
				private static final long serialVersionUID = 1L;
			}.getType();
			String varValueString = gson.toJson(variableValueMap, varValueType);
			List<Object> listToEmit1 = new ArrayList<Object>();
			listToEmit1.add(input.getValueByField("l_id"));
			listToEmit1.add(varValueString);
			listToEmit1.add(feedType);
			LOGGER.debug(" @@@ SYW PARSING BOLT EMITTING: " + listToEmit1 + " lid: " + lId);
			this.outputCollector.emit("persist_stream", listToEmit1);

			List<Object> listToEmit2 = new ArrayList<Object>();
			Map<String, String> map = formMapForScoringBolt(feedType, lId, allBoostValuesMap, variableValueMap);
			listToEmit2.add(lId);
			listToEmit2.add(gson.toJson(map, varValueType));
			listToEmit2.add(feedType);
			listToEmit2.add(lyl_id_no);
			this.outputCollector.emit("score_stream", listToEmit2);
			redisCountIncr("successful");

		} else {
			redisCountIncr("empty_var_map");
		}
		this.outputCollector.ack(input);
	}

	public Map<String, String> formMapForScoringBolt(String feedType, String lId, Map<String, Map<String, List<String>>> allBoostValuesMap, Map<String, String> variableValueMap) {
		Map<String, String> varValToScore = new HashMap<String, String>();
		try{
		Map<String, String> memberScores = memberScoreDao.getMemberScores(lId);
		Map<String, ChangedMemberScore> changedMemberScores = changedMemberScoresDao.getChangedMemberScores(lId);
		Map<String, Integer> sywBoostModelMap = modelBoostDao.getVarModelMap();
		Map<Integer, Map<Integer, Double>> modelPercentileMap = modelPercentileDao.getModelPercentiles();
		for(String variableName : variableValueMap.keySet()){
			
			Object modelIdObj= sywBoostModelMap.get(variableName);
			if (modelIdObj instanceof Integer) {
				int modelId = (Integer)modelIdObj;
				String modelIdStr = String.valueOf(modelId);
				if(memberScores.get(modelIdStr) == null || modelPercentileMap.get(modelId) == null){
					continue;
				}
				double memberScore = Double.valueOf(memberScores.get(modelIdStr));
				int percentile = getPercentileScore(variableName, allBoostValuesMap, feedType);
				if(percentile == 0){
					LOGGER.error("New "+variableName+" boost value failed to get recorded in memberBoosts collection or allBoostValuesMap for user "+lId);
					continue;
				}
				double percentileScore = modelPercentileMap.get(modelId).get(percentile);
				double val = 0.0;
				
				if ("SYW_LIKE".equals(feedType) || "SYW_WANT".equals(feedType)) {
//					Find the current memberScore
//					Find the 90% percentile score
//					Set the difference of 90% percentile score and memberScore as value of variable. If difference is below 0, set it as 0
//					Set intercept = 0, coefficient = 1
//					If member has changedmemberscores from other feeds, they would end up slightly above 9
					val = percentileScore - memberScore;
					if(val < 0)
						val = 0.0;
					varValToScore.put(variableName, String.valueOf(val));
					if("SYW_LIKE".equals(feedType))
						redisCountIncr("type_like");
					if("SYW_WANT".equals(feedType))
						redisCountIncr("type_want");
				} else if ("SYW_OWN".equals(feedType)) {
					double greater = memberScore;
					double changedMemberScore = 0.0;
					ChangedMemberScore changedMemberScoreObject = changedMemberScores.get(modelIdStr);
					if(changedMemberScoreObject != null){
						changedMemberScore = changedMemberScoreObject.getScore();
						greater = Math.max(memberScore, changedMemberScore);
					}
					if(greater >= percentileScore){
						val = percentileScore - greater;//This will be negative
					}
					varValToScore.put(variableName, String.valueOf(val));
					redisCountIncr("type_own");
				}
			}
		}
		
		}
		catch(Exception e){
			LOGGER.error("Exception in ProcessSYWInteractions for " + lId );
		}
		return varValToScore;
	}
	
	
	public int getPercentileScore(String variableName, Map<String, Map<String, List<String>>> allBoostValuesMap, String interactionType){
		Map<String, List<String>> boostValueMap = allBoostValuesMap.get(variableName);
		int numberOfEntries = 0;
		if(boostValueMap != null){
			for(Entry<String, List<String>> entry : boostValueMap.entrySet()){
				if("current".equals(entry.getKey())){
					numberOfEntries += 1;
					continue;
				}
				if(entry.getValue() != null){
					for(String num : entry.getValue()){
						numberOfEntries += Integer.valueOf(num);
					}
				}
			}
		}
		
		if(numberOfEntries == 0){
			return 0;
		}
		
		if("SYW_LIKE".equals(interactionType)){
			switch(numberOfEntries){
				case 1 : case 2:
					return 90;
				case 3: case 4:
					return 91;
				case 5: case 6:
					return 92;
				case 7: case 8:
					return 93;
				//numberOfEntries is always greater than or equal to 0, so default is equivalent to numberOfEntries >8
				default:
					return 94;
			}
		}
		else if("SYW_WANT".equals(interactionType)){
			switch(numberOfEntries){
			case 1 : case 2:
				return 95;
			case 3: case 4:
				return 96;
			case 5: case 6:
				return 97;
			case 7: case 8:
				return 98;
			//numberOfEntries is always greater than or equal to 0, so default is equivalent to numberOfEntries >8
			default:
				return 99;
			}
			
		}
		else if("SYW_OWN".equals(interactionType)){
			return 50;
		}
		return 0;
	}

	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("score_stream", new Fields("l_id", "lineItemAsJsonString", "source","lyl_id_no"));;
		declarer.declareStream("persist_stream", new Fields("l_id", "lineItemAsJsonString", "source"));
	}

	private String createJsonDoc(Map<String, List<String>> dateValuesMap) {
		LOGGER.trace("dateValuesMap: " + dateValuesMap);
		// Create string in JSON format to emit
		Gson gson = new Gson();
		Type boostValueType = new TypeToken<Map<String, List<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();

		return gson.toJson(dateValuesMap, boostValueType);
	}

}
