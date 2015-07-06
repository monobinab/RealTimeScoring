package analytics.bolt;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import analytics.util.MongoNameConstants;
import analytics.util.PidMatchUtils;
import analytics.util.dao.BoostDao;
import analytics.util.dao.DivLnVariableDao;
import analytics.util.dao.MemberBoostsDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.DivLn;
import analytics.util.objects.Variable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

public class ParsingBoltAAM_Browse extends ParseAAMFeeds {

	/**
	 * Created by Rock Wasserman 6/19/2014
	 */
	private DivLnVariableDao divLnVariableDao;
	private VariableDao variableDao;
	private MemberBoostsDao memberBoostsDao;
	private HashMap<String, List<String>> divLnBoostVariblesMap;
	private Map<String,Variable>boostMap;
	private PidMatchUtils pidMatchUtil;
	private List<String> boostList;
	
	public ParsingBoltAAM_Browse (String systemProperty, String topic) {
		super(systemProperty, topic);
			
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		pidMatchUtil = new PidMatchUtils();
		// topic is chosen to populate divLnBoostVariblesMap with source
		// specific variables
		if ("AAM_CDF_ATCProducts".equalsIgnoreCase(topic)) {
			sourceTopic = "ATC";
		} else if ("AAM_CDF_Products".equalsIgnoreCase(topic)) {
			sourceTopic = "BROWSE";
		}
		divLnVariableDao = new DivLnVariableDao();
		//divLnBoostDao = new DivLnBoostDao();
		//boostDao = new BoostDao();
		variableDao = new VariableDao();
		memberBoostsDao = new MemberBoostsDao();
		
		//populate divLnBoostvariablesMap & Boost list
		divLnBoostVariblesMap = divLnVariableDao.getDivLnBoostVariable();
			
		boostList = new ArrayList<String>();
		List<Variable> variableList = variableDao.getVariables();
		boostMap = new HashMap<String, Variable>();
		for(Variable v: variableList) {
			if(v.getName().contains(MongoNameConstants.BROWSE_BOOST_PREFIX)) {
				boostMap.put(v.getName(),v);
				boostList.add(v.getName());
			}
//			if(boostList.contains(v.getName())) {
//				boostMap.put(v.getName(),v);
//			}
		}
	}

	@Override
	protected Map<String, String> processList(String current_l_id) {
		Map<String, String> variableValueMap = new HashMap<String, String>();
		Map<String, List<String>> boostValuesMap = new HashMap<String, List<String>>();
		
		for (String pid : l_idToValueCollectionMap.get(current_l_id)) {
			// query MongoDB for division and line associated with the pid
			DivLn divLnObj = pidMatchUtil.getDivInformation(pid);
			if(divLnObj == null) {
				continue;
			}
			if(divLnBoostVariblesMap.containsKey(divLnObj.getDiv())) {
				for(String b: divLnBoostVariblesMap.get(divLnObj.getDiv())) {
					if (boostList.contains(b))
					{
							if(!boostValuesMap.containsKey(b)) {
							boostValuesMap.put(b, new ArrayList<String>());
						}
						boostValuesMap.get(b).add(pid);
					}
				}
			}
			if(divLnBoostVariblesMap.containsKey(divLnObj.getDivLn())) {
				for(String b: divLnBoostVariblesMap.get(divLnObj.getDivLn())) {
					if (boostList.contains(b)){
						if(!boostValuesMap.containsKey(b)) {
							boostValuesMap.put(b, new ArrayList<String>());
						}
						boostValuesMap.get(b).add(pid);
					}
				}
			}
		}
		Map<String, Map<String, List<String>>> allBoostValuesMap = memberBoostsDao.getMemberBoostsValues(current_l_id, boostValuesMap.keySet());
		if(allBoostValuesMap==null){
			allBoostValuesMap = new HashMap<String, Map<String, List<String>>>();
		}
		
		for(String b: boostValuesMap.keySet()) {
			if(allBoostValuesMap.containsKey(b)) {
				allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
			} else {
				allBoostValuesMap.put(b,new HashMap<String, List<String>>());
				allBoostValuesMap.get(b).put("current", boostValuesMap.get(b));
			}
			variableValueMap.put(b, createJsonDoc(allBoostValuesMap.get(b)));
		}
		
		
			//Map<String, Map<String, Integer>>memberBoostMap = new MemberBoostDao.getMemberBoosts(sourceTopic);

//			if (divLnObj != null) {
//				// get division and division/line concatenation from query
//				// results
//				String div = divLnObj.getDiv();
//				String divLn = divLnObj.getDivLn();
//				Collection<String> var = new ArrayList<String>();
//				if (divLnBoostVariblesMap.containsKey(div)) {
//					var = divLnBoostVariblesMap.get(div);
//					for (String v : var) {
//						if (variableValueMap.containsKey(var)) {
//							int value = 1 + Integer.valueOf(variableValueMap
//									.get(v));
//							variableValueMap.remove(v);
//							variableValueMap.put(v, String.valueOf(value));
//						} else {
//							variableValueMap.put(v, "1");
//						}
//					}
//				}
//				if (divLnBoostVariblesMap.containsKey(divLn)) {
//					var = divLnBoostVariblesMap.get(divLn);
//					for (String v : var) {
//						if (variableValueMap.containsKey(var)) {
//							int value = 1 + Integer.valueOf(variableValueMap
//									.get(v));
//							variableValueMap.remove(v);
//							variableValueMap.put(v, String.valueOf(value));
//						} else {
//							variableValueMap.put(v, "1");
//						}
//					}
//				}
//			}
		return variableValueMap;
	}
	
    private String createJsonDoc(Map<String, List<String>> dateValuesMap) {
    	LOGGER.debug("dateValuesMap: " + dateValuesMap);
		// Create string in JSON format to emit
    	Gson gson = new Gson();
    	Type boostValueType = new TypeToken<Map<String, List<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
		return gson.toJson(dateValuesMap, boostValueType);
	}

	@Override
	protected String[] splitRec(String webRec) {
		//TODO: See if other fields in the record are relevant. It was anyway not being used, so made this change
    	webRec = webRec.replaceAll("[']",""); 
	        String split[]=StringUtils.split(webRec,",");
	       
	        if(split !=null && split.length>0) {
	        	String [] splits = new String[split.length-2];
	        	for(int i=0; i<split.length-2; i++){
	        		splits[i]=split[i+1];
	        	}
	            return  splits;
			}
			else {
				return null;
			}
		}
}
