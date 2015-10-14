
package analytics.bolt;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import analytics.util.MongoNameConstants;
import analytics.util.PidMatchUtils;
import analytics.util.dao.DivLnBuSubBuDao;
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
	private DivLnBuSubBuDao divLnBuSubBuDao;
	private HashMap<String, List<String>> divLnBoostVariblesMap;
	private Map<String, String> divLnBuSubBuMap;
	private Map<String,Variable>boostMap;
	private PidMatchUtils pidMatchUtil;
	private List<String> boostList;
	
	public ParsingBoltAAM_Browse (String systemProperty, String source) {
		super(systemProperty, source);
			
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
	
		divLnVariableDao = new DivLnVariableDao();
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
		}
		
		divLnBuSubBuDao = new DivLnBuSubBuDao();
		divLnBuSubBuMap = divLnBuSubBuDao.getDvLnBuSubBu();
	}

	@Override
	protected Map<String, String> processList(String current_l_id, Map<String, Integer> buSubBuMap) {
		Map<String, String> variableValueMap = new HashMap<String, String>();
		Map<String, List<String>> boostValuesMap = new HashMap<String, List<String>>();
		
		Collection<String> pidsCollection = l_idToValueCollectionMap.get(current_l_id);
    	
    	if(pidsCollection==null || pidsCollection.isEmpty()|| (pidsCollection.toArray())[0].toString().trim().equalsIgnoreCase(""))
    		return null;
    	
    	LOGGER.info(current_l_id + " has " + pidsCollection.size() + " pids");
    	//System.out.println(current_l_id + " has " + pidsCollection.size() + " pids");
		for (String pid : l_idToValueCollectionMap.get(current_l_id)) {
			// query MongoDB for division and line associated with the pid
			DivLn divLnObj = pidMatchUtil.getDivInformation(pid);
			if(divLnObj == null) {
				LOGGER.info("No Div Info found for Pid : " + pid);
				continue;
			}
			
		//populate tagsMap for BrowseTags
		String buSubBu = divLnBuSubBuMap.get(divLnObj.getDivLn());
		if(buSubBu != null){
			if(!buSubBuMap.containsKey(divLnBuSubBuMap.get(divLnObj.getDivLn())))
				buSubBuMap.put(divLnBuSubBuMap.get(divLnObj.getDivLn()), 1);
			else{
				int count = (buSubBuMap.get(divLnBuSubBuMap.get(divLnObj.getDivLn()))) + 1;
				buSubBuMap.put(divLnBuSubBuMap.get(divLnObj.getDivLn()), count);
			}
		}
		//variableValueMap populating for scoring	
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
	   	webRec = webRec.replaceAll("[']",""); 
	        String split[]=StringUtils.split(webRec,",");
	       
	        if(split !=null && split.length>0) {
	        	return  split;
			}
			else {
				return null;
			}
		}
}
