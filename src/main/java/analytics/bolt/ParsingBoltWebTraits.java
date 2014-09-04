package analytics.bolt;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class ParsingBoltWebTraits extends ParseAAMFeeds {
	/**
	 * Created by Rock Wasserman 4/18/2014
	 */

    private DBCollection memberTraitsCollection;
    private DBCollection traitVariablesCollection;
    protected Map<String,Collection<String>> traitVariablesMap;
    protected Map<String,Collection<String>> variableTraitsMap;
    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		sourceTopic="WebTraits";
        

        System.out.println("PREPARING PARSING BOLT FOR WEB TRAITS");
        // POPULATE THE TRAIT TO VARIABLES MAP AND THE VARIABLE TO TRAITS MAP
        memberTraitsCollection = db.getCollection("memberTraits");
        traitVariablesCollection = db.getCollection("traitVariables");
        traitVariablesMap = new HashMap<String, Collection<String>>();
        variableTraitsMap = new HashMap<String, Collection<String>>();
		
//		System.out.println("*** TRAIT TO VARIABLES MAP >>>");
//		System.out.println(traitVariablesMap);
//		System.out.println(" *** ");
//		System.out.println(" *** ");
		

		DBCursor traitVarCursor = traitVariablesCollection.find();
		
		for(DBObject traitVariablesDBO: traitVarCursor) {
			BasicDBList variables = (BasicDBList) traitVariablesDBO.get("v");
			for(Object v:variables) {
				String variable = v.toString().toUpperCase();
				if(traitVariablesMap.containsKey(traitVariablesDBO.get("t"))) {
					if(!traitVariablesMap.get(traitVariablesDBO.get("t")).contains(variable)) {
						traitVariablesMap.get(traitVariablesDBO.get("t")).add(variable);
					}
				}
				else {
					Collection<String> newTraitVariable = new ArrayList<String>();
					traitVariablesMap.put(traitVariablesDBO.get("t").toString(), newTraitVariable);
					traitVariablesMap.get(traitVariablesDBO.get("t")).add(variable);
				}
				if(variableTraitsMap.containsKey(variable)) {
					if(!variableTraitsMap.get(variable).contains(traitVariablesDBO.get("t"))) {
						variableTraitsMap.get(variable).add(traitVariablesDBO.get("t").toString());
					}
				}
				else {
					Collection<String> newVariableTraits = new ArrayList<String>();
					variableTraitsMap.put(variable, newVariableTraits);
					variableTraitsMap.get(variable).add(traitVariablesDBO.get("t").toString());
				}
			}
		}
    }

    

	//TODO: Generalize with parsing bolt aam atc - processPidList
	//[2014-29-08]:{Trait1,Trait2}, [2014-28-08]:{Trait3,Trait2}
    protected Map<String,String> processList(String current_l_id) {
    	Map<String, Collection<String>> dateTraitsMap  = new HashMap<String,Collection<String>>(); // MAP BETWEEN DATES AND SET OF TRAITS - HISTORICAL AND CURRENT TRAITS
		List<String> variableList = new ArrayList<String>();
    	boolean firstTrait = true; //flag to indicate if the AMM trait found is the first for that member - if true then populate the memberTraitsMap
    	int traitCount = 0;
    	int variableCount = 0;
    	
    	//FOR EACH TRAIT FOUND FROM AAM DATA FIND THE VARIABLES THAT ARE IMPACTED
    	for(String trait: l_idToValueCollectionMap.get(current_l_id)) {
    		if(traitVariablesMap.containsKey(trait) && hasModelVariable(traitVariablesMap.get(trait))) {
    			if(firstTrait) {
    				prepareDateTraitsMap(dateTraitsMap, current_l_id);
    				firstTrait = false;
    			}
    			
				if(addTraitToDateTraitMap(trait, dateTraitsMap)) {
					traitCount++;
	    			for(String variable: traitVariablesMap.get(trait)) {
		    			if(modelVariablesList.contains(variable.toUpperCase()) && !variableList.contains(variable.toUpperCase())) {
		    				variableList.add(variable.toUpperCase());
		    				variableCount++;
		    			}
	    			}
	    		}
    		}
    	}
		System.out.println(" traits found: " + traitCount + " ... variables found: " + variableCount);
		Map<String,String> variableDateTraitMap = new HashMap<String, String>();
    	if(dateTraitsMap != null && !dateTraitsMap.isEmpty() && !variableList.isEmpty()) {
    		for(String v : variableList) {
        	String dateTraitString = createJsonFromDateTraitsMap(dateTraitsMap);
        	variableDateTraitMap.put(v, dateTraitString);
    		}
    		return variableDateTraitMap;
    	}
    	else {
    		return null;
    	}
    }
    
    private String createJsonFromDateTraitsMap(Map<String, Collection<String>> stringCollectionMap) {
		System.out.println("dateTraitMap: " + stringCollectionMap);
		// Create string in JSON format to emit
    	Gson gson = new Gson();
    	Type dateTraitValueType = new TypeToken<Map<String, Collection<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
		String dateTraitString = gson.toJson(stringCollectionMap, dateTraitValueType);
		return dateTraitString;
	}
	

	private boolean addTraitToDateTraitMap(String trait, Map<String, Collection<String>> dateTraitsMap) {
		boolean addedTrait = false;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		if(!dateTraitsMap.containsKey(simpleDateFormat.format(new Date()))) {
			dateTraitsMap.put(simpleDateFormat.format(new Date()), new ArrayList<String>());
			dateTraitsMap.get(simpleDateFormat.format(new Date())).add(trait);
			System.out.println(" added trait: " + trait);
			addedTrait=true;
		}
		else if(!dateTraitsMap.get(simpleDateFormat.format(new Date())).contains(trait)) {
			dateTraitsMap.get(simpleDateFormat.format(new Date())).add(trait);
			System.out.println(" added trait: " + trait);
			addedTrait=true;
		}
		return addedTrait;
	}
    
    
    private void prepareDateTraitsMap(Map<String, Collection<String>> dateTraitsMap, String current_l_id) {
		DBObject memberTraitsDBO = memberTraitsCollection.findOne(new BasicDBObject().append("l_id", current_l_id));
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		if(memberTraitsDBO != null && memberTraitsDBO.keySet().contains(current_l_id)) {
			
			BasicDBList dates = (BasicDBList) memberTraitsDBO.get("date");
			
			for( Iterator<Object> dateIterator = dates.iterator(); dateIterator.hasNext(); ) {
				BasicDBObject dateDBO = (BasicDBObject) dateIterator.next();
				try {
					if(simpleDateFormat.parse(dateDBO.get("d").toString()).after(new Date(new Date().getTime() + (-7 * 1000 * 60 * 60 * 24)))) {
						Collection<String> newTraitsCollection = new ArrayList<String>();
						dateTraitsMap.put(dateDBO.get("d").toString(), newTraitsCollection);
						BasicDBList traitsDBList = (BasicDBList) dateDBO.get("t");
						if(traitsDBList != null && !traitsDBList.isEmpty()) {
							for( Iterator<Object> tIterator = traitsDBList.iterator(); tIterator.hasNext(); ) {
								Object t = tIterator.next();
								dateTraitsMap.get(dateDBO.get("d").toString()).add(t.toString());
							}
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}
    
    @Override
    protected String[] splitRec(String webRec) {
    	//TODO: Do not use regex. Have a better way. This is temp
    	webRec = webRec.replaceAll("['\\[\\]\"]",""); 
        //System.out.println("WEB RECORD: " + webRec);
        String split[]=StringUtils.split(webRec,",");
        
        if(split !=null && split.length>0) {
			return split;
		}
		else {
			return null;
		}
	}
    
    
}
