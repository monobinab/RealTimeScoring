package analytics.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.LocalDate;

import analytics.util.JsonUtils;
import analytics.util.dao.ChangedMemberScoresDao;
import analytics.util.dao.MemberScoreDao;
import analytics.util.dao.ModelSywBoostDao;
import analytics.util.objects.Change;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SywScoringBolt  extends BaseRichBolt{
	private ModelSywBoostDao modelBoostDao;
	private ChangedMemberScoresDao changedMemberScoresDao;
	private MemberScoreDao memberScoreDao;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		modelBoostDao = new ModelSywBoostDao();
		memberScoreDao = new MemberScoreDao();
		changedMemberScoresDao = new ChangedMemberScoresDao();
	}

	@Override
	public void execute(Tuple input) {
		String lId = input.getStringByField("l_id");
		if(lId.equals("dxo0b7SN1eER9shCSj0DX+eSGag=")){
			System.out.println("ME");
		}
		String source = input.getStringByField("source");
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}

		// 2) Create map of new changes from the input
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));
		Map<String,Integer> varToCountMap = new HashMap<String, Integer>();
		for (String variableName : newChangesVarValueMap.keySet()) {
	    	Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson(newChangesVarValueMap.get(variableName));
	    	int totalPidCount = 0;
	    	
	    	if(dateValuesMap != null && dateValuesMap.containsKey("current")) {
		    	for(String v: dateValuesMap.get("current")) {
		    		totalPidCount++;
		    	}
		    	dateValuesMap.remove("current");
		    	if(!dateValuesMap.isEmpty()) {
		    		for(String key: dateValuesMap.keySet()) {
		    			try {
							if(!new Date().after(new LocalDate(simpleDateFormat.parse(key)).plusDays(7).toDateMidnight().toDate()))
							for(String v: dateValuesMap.get(key)) {
								totalPidCount+=Integer.valueOf(v);
							}
						} catch (NumberFormatException e) {
							e.printStackTrace();
						} catch (ParseException e) {
							e.printStackTrace();
						}
		    		}
		    	}
	    	}
	    	varToCountMap.put(variableName, totalPidCount);
		}
		//boost_syw... hand_tools_tcount
		//boost_syw... tools_tcount
		List<Integer> modelIds = new ArrayList<Integer>();
		for(String variableName:varToCountMap.keySet()){
			//Change dao to take in multiple variable names and return list of modelIds
			Integer modelId = modelBoostDao.getModelId(variableName);
			modelIds.add(modelId);
			//Get changed member score for this modelId
			//If this is null/expired, get the memberScore
			
		}
		System.out.println("rescored all models in modelIds list");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
