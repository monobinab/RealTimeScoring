package analytics.bolt;

import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Variable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PersistBoostsBolt extends EnvironmentBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(PersistBoostsBolt.class);
    private MemberBoostsDao memberBoostsDao;
    private VariableDao variableDao;
    private OutputCollector outputCollector;
    Map<String, String> variablesStrategyMap ;
	 public PersistBoostsBolt(String systemProperty){
		 super(systemProperty);
	}
	 
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
    	memberBoostsDao = new MemberBoostsDao();
		variableDao = new VariableDao();
		variablesStrategyMap = new HashMap<String, String>();
		for(Variable v: variableDao.getAllVariables()) {
  			if(v.getName().substring(0, 5).equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
  				variablesStrategyMap.put(v.getName(), v.getStrategy());
  			}
  		}
	}

	@Override
	public void execute(Tuple input) {
	
		String lyl_id_no = null;
		String l_id = null;
		try{
			redisCountIncr("incoming_record");
	        Map<String, Map<String, List<String>>> memberBoostValuesMap = new HashMap<String, Map<String, List<String>>>();
	   
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			lyl_id_no = input.getStringByField("lyl_id_no");
			l_id = input.getStringByField("l_id");
			Map<String, String> newChangesVarValueMap = JsonUtils
					.restoreVariableListFromJson(input.getString(1));
	        for(String b: newChangesVarValueMap.keySet()) {
		    	Map<String, List<String>> dateValuesMap = JsonUtils.restoreDateTraitsMapFromJson(newChangesVarValueMap.get(b));
	            LOGGER.trace(" dateValuesMap beginning for " +b +" : " + dateValuesMap + " lid: " + l_id);
	            List<String> persistValuesList = new ArrayList<String>();
		    
		    	if(variablesStrategyMap.get(b).equals("StrategyBoostProductTotalCount")) {
		    		persistValuesList = getTotalCount(dateValuesMap);
		    		if(persistValuesList!= null && !persistValuesList.isEmpty()) {
		    			dateValuesMap.put(simpleDateFormat.format(new Date()), persistValuesList);
	                    LOGGER.trace(" dateValuesMap  for " +b +" in StrategyBoostProductTotalCount : " + dateValuesMap + " lid: " + l_id);
	
	                }
		    	}
	         	if(variablesStrategyMap.get(b).equals("StrategySywTotalCounts")) {
		    		persistValuesList = getTotalCount(dateValuesMap);
		    		if(persistValuesList!= null && !persistValuesList.isEmpty()) {
		    			dateValuesMap.put(simpleDateFormat.format(new Date()), persistValuesList);
	                    LOGGER.trace(" dateValuesMap  for " +b +" in StrategySywTotalCounts : " + dateValuesMap + " lid: " + l_id);
	                }
		    	}
	            LOGGER.trace(" dateValuesMap end for " +b +" : " + dateValuesMap + " lid: " + l_id);
	            if(dateValuesMap != null && dateValuesMap.size() > 0){
	            	memberBoostValuesMap.put(b,dateValuesMap);
	            }
			}
	        if(memberBoostValuesMap != null && memberBoostValuesMap.size() > 0){
	        	memberBoostsDao.writeMemberBoostValues(l_id, memberBoostValuesMap);
	        	redisCountIncr("persisted_boost");
	        }
		}
		catch(Exception e){
			LOGGER.error("Exception occured in PersistBoostbolt " + e.getMessage());
		}
			this.outputCollector.ack(input);
			LOGGER.info("PERSIST: " + lyl_id_no + " gets acked in PersistBoostsBolt"); 
	}

	private List<String> getTotalCount(Map<String, List<String>> dateValuesMap) {
    	
		List<String> totalCountList = new ArrayList<String>();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	int totalPidCount = 0;
    	
    	if(dateValuesMap != null && dateValuesMap.containsKey("current")) {
    		totalPidCount+=dateValuesMap.get("current").size();
	    	dateValuesMap.remove("current");
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
