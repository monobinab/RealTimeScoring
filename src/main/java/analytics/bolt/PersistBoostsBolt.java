package analytics.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Variable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class PersistBoostsBolt extends BaseRichBolt {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistTraitsBolt.class);
    private MemberBoostsDao memberBoostsDao;
    private VariableDao variableDao;
    private Map<String, String> variablesStrategyMap;
    Map<String, Map<String, List<String>>> memberBoostValuesMap;
    
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		memberBoostValuesMap = new HashMap<String, Map<String, List<String>>>();
		memberBoostsDao = new MemberBoostsDao();
		variableDao = new VariableDao();
		variablesStrategyMap = new HashMap<String, String>();
		
		//populate variablesStrategyMap
		for(Variable v: variableDao.getVariables()) {
			if(v.getName().substring(0, 5).equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
				variablesStrategyMap.put(v.getName(), v.getStrategy());
			}
		}
	}

	@Override
	public void execute(Tuple input) {

		LOGGER.debug("Persisting boost + values in mongo");
		//Get the encrypted loyalty id
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String l_id = input.getString(0);
		
		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));
		
		for(String b: newChangesVarValueMap.keySet()) {
	    	Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson(newChangesVarValueMap.get(b));
	    	List<String> persistValuesList = new ArrayList<String>();
	    
	    	if(variablesStrategyMap.get(b).equals("StrategyBoostProductTotalCount")) {
	    		persistValuesList = getTotalCount(dateValuesMap);
	    		if(persistValuesList!= null && !persistValuesList.isEmpty()) {
	    			dateValuesMap.put(simpleDateFormat.format(new Date()), persistValuesList);
	    		}
	    	}
	    	//
	    	if(variablesStrategyMap.get(b).equals("StrategySywTotalCounts")) {
	    		persistValuesList = getTotalCount(dateValuesMap);
	    		if(persistValuesList!= null && !persistValuesList.isEmpty()) {
	    			dateValuesMap.put(simpleDateFormat.format(new Date()), persistValuesList);
	    		}
	    	}
	    	
	    	memberBoostValuesMap.put(b,dateValuesMap);
	    	//addDateTrait
		}
		new MemberBoostsDao().writeMemberBoostValues(l_id, memberBoostValuesMap);
	}

	private List<String> getTotalCount(Map<String, List<String>> dateValuesMap) {
    	
		List<String> totalCountList = new ArrayList<String>();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	int totalPidCount = 0;
    	
    	if(dateValuesMap != null && dateValuesMap.containsKey("current")) {
	    	for(String val: dateValuesMap.get("current")) {
	    		totalPidCount++;
	    	}
	    	if(dateValuesMap.containsKey(simpleDateFormat.format(new Date()))) {
				for(String v: dateValuesMap.get(simpleDateFormat.format(new Date()))) {
					totalPidCount+=Integer.valueOf(v);
	    		}
	    	}
	    	totalCountList.add(String.valueOf(totalPidCount));
	    	return totalCountList;
    	} else {
    		return null;
    	}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
