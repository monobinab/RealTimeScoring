package analytics.bolt;

import analytics.util.HostPortUtility;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Variable;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class PersistBoostsBolt extends BaseRichBolt {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PersistBoostsBolt.class);
    private MemberBoostsDao memberBoostsDao;
    private VariableDao variableDao;
    private Map<String, String> variablesStrategyMap;
	private MultiCountMetric countMetric;
	 void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
     //   System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
		   HostPortUtility.getEnvironment(stormConf.get("nimbus.host").toString());
		memberBoostsDao = new MemberBoostsDao();
		variableDao = new VariableDao();
		variablesStrategyMap = new HashMap<String, String>();
		
		//populate variablesStrategyMap
		for(Variable v: variableDao.getVariables()) {
			if(v.getName().substring(0, 5).equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
				variablesStrategyMap.put(v.getName(), v.getStrategy());
			}
		}
		initMetrics(context);
	}

	@Override
	public void execute(Tuple input) {

		countMetric.scope("incoming_record").incr();
        Map<String, Map<String, List<String>>> memberBoostValuesMap = new HashMap<String, Map<String, List<String>>>();

        LOGGER.debug("Persisting boost + values in mongo");
		//Get the encrypted loyalty id
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String l_id = input.getString(0);
		
		Map<String, String> newChangesVarValueMap = JsonUtils
				.restoreVariableListFromJson(input.getString(1));

        LOGGER.trace("Sent to Persist Bolt " + newChangesVarValueMap + " lid: " + l_id);


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


	    	//
	    	if(variablesStrategyMap.get(b).equals("StrategySywTotalCounts")) {
	    		persistValuesList = getTotalCount(dateValuesMap);
	    		if(persistValuesList!= null && !persistValuesList.isEmpty()) {
	    			dateValuesMap.put(simpleDateFormat.format(new Date()), persistValuesList);
                    LOGGER.trace(" dateValuesMap  for " +b +" in StrategySywTotalCounts : " + dateValuesMap + " lid: " + l_id);

                }
	    	}
            LOGGER.trace(" dateValuesMap end for " +b +" : " + dateValuesMap + " lid: " + l_id);

            memberBoostValuesMap.put(b,dateValuesMap);
	    	//addDateTrait
		}

        LOGGER.trace("Sent to DAO " + memberBoostValuesMap + " lid: " + l_id);


        new MemberBoostsDao().writeMemberBoostValues(l_id, memberBoostValuesMap);
		countMetric.scope("persisted_boost").incr();
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
