package analytics.bolt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ParsingBoltAAM_ATC extends ParseAAMFeeds {

	/**
	 * Created by Rock Wasserman 6/19/2014
	 */

	DBCollection pidDivLnCollection;
	DBCollection divLnBoostVariableCollection;
	private HashMap<String, List<String>> divLnBoostVariblesMap;
	
	private String currentUUID;

	public ParsingBoltAAM_ATC() {
		super();
	}

	public ParsingBoltAAM_ATC(String topic) {
		super(topic);
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

		pidDivLnCollection = db.getCollection("pidDivLn");
		divLnBoostVariableCollection = db.getCollection("divLnBoost");

		// topic is chosen to populate divLnBoostVariblesMap with source
		// specific variables
		if (topic.equalsIgnoreCase("AAM_CDF_ATCProducts")) {
			sourceTopic = "ATC";
		} else if (topic.equalsIgnoreCase("AAM_CDF_Products")) {
			sourceTopic = "BROWSE";
		}

		// //populate divLnBoostvariablesMap
		DBCursor divLnBoostVarCursor = divLnBoostVariableCollection.find();
		logger.debug("cursor size: " + divLnBoostVarCursor.size());
		divLnBoostVariblesMap = new LinkedHashMap<String, List<String>>();
		for (DBObject divLnBoostDBObject : divLnBoostVarCursor) {

			@SuppressWarnings("unchecked")
			List<HashMap> hashMaps = (List<HashMap>) divLnBoostDBObject
					.get("v");
			List<String> varibles = new ArrayList<String>();

			for (HashMap hashMap : hashMaps) {
				if (hashMap.containsKey(sourceTopic)) {
					varibles.add((String) hashMap.get(sourceTopic));
				}
			}
			if (varibles.size() > 0
					&& !(divLnBoostVariblesMap.containsKey(divLnBoostDBObject
							.get("d")))) {

				divLnBoostVariblesMap.put((String) divLnBoostDBObject.get("d"),
						varibles);
			} else if (varibles.size() > 0) {
				List<String> variblevalues = divLnBoostVariblesMap
						.get(divLnBoostDBObject.get("d"));
				varibles.addAll(variblevalues);
				divLnBoostVariblesMap.put((String) divLnBoostDBObject.get("d"),
						varibles);
			}

		}
	}


	/*
	 * private Map<String,String> processPidList() { Map<String,String>
	 * variableValueMap = new HashMap<String,String>();
	 * 
	 * for(String pid: l_idToPidCollectionMap.get(current_l_id)) { //query
	 * MongoDB for division and line associated with the pid DBObject
	 * pidDivLnQueryResult = pidDivLnCollection.findOne(new
	 * BasicDBObject().append("pid", pid)); if(pidDivLnQueryResult != null) {
	 * //get division and division/line concatenation from query results String
	 * div = pidDivLnQueryResult.get("d").toString(); String divLn =
	 * pidDivLnQueryResult.get("l").toString(); Collection<String> var = new
	 * ArrayList<String>(); if(divLnVariablesMap.containsKey(div)) { var =
	 * divLnVariablesMap.get(div); for(String v:var) {
	 * if(variableValueMap.containsKey(var)) { int value = 1 +
	 * Integer.valueOf(variableValueMap.get(v)); variableValueMap.remove(v);
	 * variableValueMap.put(v, String.valueOf(value)); } else {
	 * variableValueMap.put(v, "1"); } } }
	 * if(divLnVariablesMap.containsKey(divLn)) { var =
	 * divLnVariablesMap.get(divLn); for(String v:var) {
	 * if(variableValueMap.containsKey(var)) { int value = 1 +
	 * Integer.valueOf(variableValueMap.get(v)); variableValueMap.remove(v);
	 * variableValueMap.put(v, String.valueOf(value)); } else {
	 * variableValueMap.put(v, "1"); } } } } } return variableValueMap; }
	 */

	
	@Override
	protected Map<String, String> processList(String current_l_id) {
		Map<String, String> variableValueMap = new HashMap<String, String>();

		for (String pid : l_idToValueCollectionMap.get(current_l_id)) {
			// query MongoDB for division and line associated with the pid
			DBObject pidDivLnQueryResult = pidDivLnCollection
					.findOne(new BasicDBObject().append("pid", pid));
			if (pidDivLnQueryResult != null) {
				// get division and division/line concatenation from query
				// results
				String div = pidDivLnQueryResult.get("d").toString();
				String divLn = pidDivLnQueryResult.get("l").toString();
				Collection<String> var = new ArrayList<String>();
				if (divLnBoostVariblesMap.containsKey(div)) {
					var = divLnBoostVariblesMap.get(div);
					for (String v : var) {
						if (variableValueMap.containsKey(var)) {
							int value = 1 + Integer.valueOf(variableValueMap
									.get(v));
							variableValueMap.remove(v);
							variableValueMap.put(v, String.valueOf(value));
						} else {
							variableValueMap.put(v, "1");
						}
					}
				}

			}
		}
		return variableValueMap;
	}
	
	@Override
	protected String[] splitRec(String webRec) {
		//TODO: See if other fields in the record are relevant. It was anyway not being used, so made this change
	        //System.out.println("WEB RECORD: " + webRec)
    	webRec = webRec.replaceAll("[']",""); 
	        String split[]=StringUtils.split(webRec,",");
	       
	        if(split !=null && split.length>0) {
	            String[] returnArray = { split[1], split[2] };
				return returnArray;
			}
			else {
				return null;
			}
		}
}
