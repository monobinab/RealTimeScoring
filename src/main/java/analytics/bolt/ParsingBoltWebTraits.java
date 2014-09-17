package analytics.bolt;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import analytics.util.JsonUtils;
import analytics.util.dao.MemberTraitsDao;
import analytics.util.dao.TraitVariablesDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;


public class ParsingBoltWebTraits extends ParseAAMFeeds {
	/**
	 * Created by Rock Wasserman 4/18/2014
	 */

    protected Map<String,List<String>> traitVariablesMap;
    protected Map<String,List<String>> variableTraitsMap;
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
        
        logger.info("PREPARING PARSING BOLT FOR WEB TRAITS");
        // POPULATE THE TRAIT TO VARIABLES MAP AND THE VARIABLE TO TRAITS MAP
        traitVariablesMap = new TraitVariablesDao().getTraitVariableList();
        variableTraitsMap = new TraitVariablesDao().getVariableTraitList();		
    }

    

	//Generalize with parsing bolt aam atc - processPidList
	//[2014-29-08]:{Trait1,Trait2}, [2014-28-08]:{Trait3,Trait2}
    protected Map<String,String> processList(String current_l_id) {
    	logger.debug("Processing list of traits");
    	Map<String, List<String>> dateTraitsMap = null; // MAP BETWEEN DATES AND SET OF TRAITS - HISTORICAL AND CURRENT TRAITS
		List<String> variableList = new ArrayList<String>();
    	boolean firstTrait = true; //flag to indicate if the AMM trait found is the first for that member - if true then populate the memberTraitsMap
    	int traitCount = 0;
    	int variableCount = 0;
    	
    	//FOR EACH TRAIT FOUND FROM AAM DATA FIND THE VARIABLES THAT ARE IMPACTED
    	logger.debug("Finding list of variables for each trait");
    	for(String trait: l_idToValueCollectionMap.get(current_l_id)) {
    		if(traitVariablesMap.containsKey(trait) && JsonUtils.hasModelVariable(modelVariablesList,traitVariablesMap.get(trait))) {
    			if(firstTrait) {
    				dateTraitsMap = new MemberTraitsDao().getDateTraits(current_l_id);
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
		logger.debug(" traits found: " + traitCount + " ... variables found: " + variableCount);
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
    
    private String createJsonFromDateTraitsMap(Map<String, List<String>> stringCollectionMap) {
    	logger.debug("dateTraitMap: " + stringCollectionMap);
		// Create string in JSON format to emit
    	Gson gson = new Gson();
    	Type dateTraitValueType = new TypeToken<Map<String, Collection<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
		String dateTraitString = gson.toJson(stringCollectionMap, dateTraitValueType);
		return dateTraitString;
	}
	

	private boolean addTraitToDateTraitMap(String trait, Map<String, List<String>> dateTraitsMap) {
		logger.debug("add trait to date trait map");
		boolean addedTrait = false;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		if(!dateTraitsMap.containsKey(simpleDateFormat.format(new Date()))) {
			dateTraitsMap.put(simpleDateFormat.format(new Date()), new ArrayList<String>());
			dateTraitsMap.get(simpleDateFormat.format(new Date())).add(trait);
			logger.trace(" added trait: " + trait);
			addedTrait=true;
		}
		else if(!dateTraitsMap.get(simpleDateFormat.format(new Date())).contains(trait)) {
			dateTraitsMap.get(simpleDateFormat.format(new Date())).add(trait);
			logger.trace(" added trait: " + trait);
			addedTrait=true;
		}
		return addedTrait;
	}
    
    @Override
    protected String[] splitRec(String webRec) {
    	logger.debug("Parsing trait record");
    	//TODO: Do not use regex. Have a better way. This is temp
    	if(webRec==null)
    		return null;
    	webRec = webRec.replaceAll("['\\[\\]\" ]",""); 
        String split[]=StringUtils.split(webRec,",");
        
        if(split !=null && split.length>0) {
			return split;
		}
		else {
			return null;
		}
	}
    
    
}
