package analytics.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.MemberScoreDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.ModelSywBoostDao;
import analytics.util.objects.ChangedMemberScore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SywScoringBolt  extends BaseRichBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SywScoringBolt.class);
	private ChangedMemberScoresDao changedMemberScoresDao;
	private MemberScoreDao memberScoreDao;
	private ModelSywBoostDao modelBoostDao;
	private ModelPercentileDao modelPercentileDao;
	
	private Map<String,Integer> boostModelMap;
	private Map<Integer, Map<Integer, Double>> modelPercentileMap;
	private SimpleDateFormat simpleDateFormat;
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		outputCollector = collector;
		modelPercentileDao = new ModelPercentileDao();
		memberScoreDao = new MemberScoreDao();
		changedMemberScoresDao = new ChangedMemberScoresDao();
		modelBoostDao = new ModelSywBoostDao();
		boostModelMap = buildBoostModelMap();
		modelPercentileMap = modelPercentileDao.getModelPercentiles();
		simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	}

	@Override
	public void execute(Tuple input) {
		//l_id="jgjh" , source="SYW_LIKE/OWN/WANT
		//newChangesVarValueMap - similar to strategy bolt
		//current-pid, 2014-09-25-[6],...
		
		String lId = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		
		//TODO: Reuse this as a function. AAM ATC uses a very similar piece of code
		// 2) Create map of new changes from the input
    	simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));
		Map<String,Integer> varToCountMap = new HashMap<String, Integer>();		
		
		for (String variableName : newChangesVarValueMap.keySet()) {
	    	Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson(newChangesVarValueMap.get(variableName));
	    	int totalPidCount = 0;
	    	
	    	if(dateValuesMap != null && dateValuesMap.containsKey("current")) {
	    		totalPidCount+=dateValuesMap.get("current").size();
		    	dateValuesMap.remove("current");
		    	if(!dateValuesMap.isEmpty()) {
		    		for(String key: dateValuesMap.keySet()) {
		    			try {
							if(!new Date().after(new LocalDate(simpleDateFormat.parse(key)).plusDays(7).toDateMidnight().toDate()))
							for(String v: dateValuesMap.get(key)) {
								totalPidCount+=Integer.valueOf(v);
							}
						} catch (NumberFormatException e) {
							LOGGER.warn(e.getMessage(),e);
						} catch (ParseException e) {
							LOGGER.warn(e.getMessage(),e);
						}
		    		}
		    	}
	    	}
	    	varToCountMap.put(variableName, totalPidCount);
		}
		//boost_syw... hand_tools_tcount
		//boost_syw... tools_tcount
		//var to Count map - 
		//varname, totalcount across all days
		
		Map<Integer,String> modelIdToScore = new HashMap<Integer, String>();
		Map<String,String> memberScores = memberScoreDao.getMemberScores(lId);
		Map<String,ChangedMemberScore> changedMemberScores = changedMemberScoresDao.getChangedMemberScores(lId);
		//Also read and keep changedMemberScores
		for(String variableName:varToCountMap.keySet()){
			//Change dao to take in multiple variable names and return list of modelIds
			Integer modelId = boostModelMap.get(variableName);
			ChangedMemberScore cs = changedMemberScores.get(modelId.toString());
			if(cs!=null){
				Date expiry;
				try {
					expiry = simpleDateFormat.parse(cs.getMinDate());
					if(expiry.after(new Date())){
						modelIdToScore.put(modelId, String.valueOf(cs.getScore()));
					} 
				} catch (ParseException e) {
					LOGGER.error("Unable to parse date", e);
				}
			}
			if(!modelIdToScore.containsKey(modelId))
				modelIdToScore.put(modelId, memberScores.get(modelId.toString()));
		}
		
		//TODO: Loop through the modelIdToScore map
		//Add scoring logic here
		//varToCount map has the total count for each variable
		
		for(String v: varToCountMap.keySet()) {
			
			int boostPercetages = 0;
			int modelId = boostModelMap.get(v);
			if(modelIdToScore==null||modelIdToScore.get(modelId)==null){
				//Getting next model since current one does not have score
				continue;
			}
			
			if(varToCountMap.get(v)<=10){
				boostPercetages += ((int) Math.ceil(varToCountMap.get(v) / 2.0))-1;
			} else {
				boostPercetages = 5;
			}
			
			Double maxScore = modelPercentileMap.get(modelId).get(90 + boostPercetages);
			String oldScore = modelIdToScore.get(modelId);
			if(Double.valueOf(modelIdToScore.get(modelId)) < maxScore) {
				
				modelIdToScore.put(modelId, maxScore.toString());
			}
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(lId);
			listToEmit.add(oldScore);
			listToEmit.add(Double.parseDouble(modelIdToScore.get(modelId)));
			listToEmit.add(String.valueOf(modelId));
			listToEmit.add(source);
			listToEmit.add(messageID);
			outputCollector.emit(listToEmit);
		}
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "oldScore", "newScore", "model",
				"source", "messageID"));
	}

	private Map<String, Integer> buildBoostModelMap() {
		
		Map<String, Integer> boostModelMap = modelBoostDao.getVarModelMap();
		return boostModelMap;
		
	}
	
	
	
	
}
