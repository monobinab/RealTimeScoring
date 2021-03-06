package analytics.bolt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.BrowseUtils;
import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.BoostBrowseBuSubBuDao;
import analytics.util.dao.DivLnModelCodeDao;
import analytics.util.dao.ModelVariablesDao;
import analytics.util.dao.SourceFeedDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.BoostBrowseBuSubBu;
import analytics.util.objects.MemberBrowse;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public abstract class ParseAAMFeeds  extends EnvironmentBolt {

	protected static final Logger LOGGER = LoggerFactory.getLogger(ParseAAMFeeds.class);
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;
    protected String source;
 	protected ModelVariablesDao modelVariablesDao;
	protected DivLnModelCodeDao divLnModelCodeDao;
	protected SourceFeedDao sourceFeedDao;
	protected VariableDao variableDao;
	protected BoostBrowseBuSubBuDao boostBrowseBuSubBuDao;
	protected BrowseUtils browseUtils;
	private static final int NUMBER_OF_DAYS = 7;

			
    public ParseAAMFeeds() {
	}
    
	public ParseAAMFeeds( String systemProperty, String source) {
		super(systemProperty);
		this.source = source;
	}
		
    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
    
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        super.prepare(stormConf, context, collector);
	    modelVariablesDao =  new ModelVariablesDao(); 
        sourceFeedDao = new SourceFeedDao();
        browseUtils = new BrowseUtils();
        divLnModelCodeDao = new DivLnModelCodeDao();
        boostBrowseBuSubBuDao = new BoostBrowseBuSubBuDao();
     }

	@Override
	public void execute(Tuple input) {
		Map<String,String> incomingValueMap;
		try{
			String loyalty_id;
		
			redisCountIncr("incoming_tuples");
			
	        String interactionRec = input.getString(0);
	        String splitRecArray[] = splitRec(interactionRec);
	        
	        if(splitRecArray == null || splitRecArray.length==0) {
	    		redisCountIncr("invalid_record");
	    		outputCollector.ack(input);
	        	return;
	        }
	      
	        String l_id = null;
	        
	        loyalty_id = splitRecArray[0].trim();
	        if(loyalty_id.length()!=16 || !loyalty_id.startsWith("7081")){
	        	LOGGER.info("Could not find Lid: " + loyalty_id);
	        	redisCountIncr("no_lids");
	        	outputCollector.ack(input);
	        	return;
	        }
		    l_id = SecurityUtils.hashLoyaltyId(loyalty_id);
	        
		    Map<String, Collection<String>> l_idToCurrentValueCollectionMap = new HashMap<String, Collection<String>>();
		    l_idToCurrentValueCollectionMap.put(l_id, new ArrayList<String>());
			for(int i=1;i<splitRecArray.length;i++){
				l_idToCurrentValueCollectionMap.get(l_id).add(splitRecArray[i].trim());
			}
			
		   	incomingValueMap = processList(l_id, l_idToCurrentValueCollectionMap); //LIST OF VARIABLES FOUND DURING TRAITS PROCESSING
		   	
			/*
			 * Get the MemberBrowse object for the member for 7 days
			 */
			MemberBrowse memberBrowse = browseUtils.getEntireMemberBrowse7DaysInHistory(l_id)	;
	    	
	    	if(incomingValueMap !=null && !incomingValueMap.isEmpty()) {
	    		Object boostValueJSON = null;
	    		if(!source.equalsIgnoreCase("WebTraits")){
	    				Map<String, Integer> previousModelCodeMap = browseUtils.getPreviousBoostCounts(l_id, loyalty_id, incomingValueMap, NUMBER_OF_DAYS, memberBrowse);
		           		LOGGER.info("PC modelCode or buSubBu Map for scoring " + l_id + ": " + previousModelCodeMap);
		        		Map<String, String> totalModelCodeMap = getTotalModelCodeValueMap(previousModelCodeMap, incomingValueMap);
		               	LOGGER.info("PC AND incoming modelCode or buSubBu Map for scoring " + l_id + ": "+ totalModelCodeMap);
		            	boostValueJSON = JsonUtils.createJsonFromStringStringMap(getBoostFromModelCode(totalModelCodeMap));
		            	emitToBrowseCountPersistBolt(incomingValueMap, loyalty_id, memberBrowse);
	   	   		}
	    		else{
	    			 boostValueJSON = JsonUtils.createJsonFromStringStringMap(incomingValueMap);
	    		}
	       		if(boostValueJSON != null ){
		        	List<Object> listToEmit = new ArrayList<Object>();
		        	listToEmit.add(l_id);
		        	listToEmit.add(boostValueJSON);
		        	listToEmit.add(source);
		        	listToEmit.add(loyalty_id);
		        	this.outputCollector.emit(listToEmit);
		        	redisCountIncr("processed_lid");
		        	LOGGER.debug(" *** PARSING BOLT EMITTING: " + listToEmit);
	       		}
	       		
	       	}
	    	else {
	    		LOGGER.debug(" *** NO VARIABLES FOUND - NOTHING TO EMIT");
	    		redisCountIncr("no_variables_affected");
	    	}
		}
		catch(Exception e){
			LOGGER.error("Exception occured in PasingBoltAAMFeeds: " + ExceptionUtils.getFullStackTrace(e));
			e.printStackTrace();
		}
	    	incomingValueMap = null;   	
	    	redisCountIncr("total_processing");
	    	this.outputCollector.ack(input);
	  }

	private void emitToBrowseCountPersistBolt(
			Map<String, String> incomingValueMap, String loyalty_id,
			MemberBrowse memberBrowse) {
			Object boostJSON = JsonUtils.createJsonFromStringStringMap(incomingValueMap);
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(loyalty_id);
			listToEmit.add(boostJSON);
			listToEmit.add(memberBrowse);
			listToEmit.add(source);
			this.outputCollector.emit("browse_tag_stream", listToEmit);
			redisCountIncr("emitted_browse_tag");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source","lyl_id_no"));
		declarer.declareStream("browse_tag_stream", new Fields("loyalty_id", "boostJSON","memberBrowse", "source"));
	}
	
    abstract protected String[] splitRec(String webRec);
	protected abstract Map<String, String> processList(String current_l_id, Map<String, Collection<String>> l_idToPidCollectionMap);
    

	
	protected Map<String, String> getTotalModelCodeValueMap(Map<String, Integer> previousModelCodeMap, Map<String, String> incomingModelCodesMap){
		Map<String, String> totalModeCode= new HashMap<String, String>();
		for(String modelCode: incomingModelCodesMap.keySet()){
			if(previousModelCodeMap != null && previousModelCodeMap.size() > 0 &&  previousModelCodeMap.containsKey(modelCode)){
				int count = previousModelCodeMap.get(modelCode) + Integer.valueOf(incomingModelCodesMap.get(modelCode));
				totalModeCode.put(modelCode, String.valueOf(count));
			}
			else{
				totalModeCode.put(modelCode, incomingModelCodesMap.get(modelCode));
			}
		}
		return totalModeCode;
	}

	protected void getIncomingModelCodeMap(String div, Map<String, String> incomingModelCodeMap){
		Map<String, List<String>> divLnModelCodeMap = divLnModelCodeDao.getDivLnModelCode();
		if (divLnModelCodeMap.containsKey(div)) {
			for (String modelCode : divLnModelCodeMap.get(div)) {
				populateIncomingModelCodeMap(incomingModelCodeMap, modelCode);
			}
		}
	}

	protected void populateIncomingModelCodeMap(
			Map<String, String> incomingModelCodeMap, String modelCode) {
		if(!incomingModelCodeMap.containsKey(modelCode)){
			incomingModelCodeMap.put(modelCode, "1");
		}
		else{
			int count = Integer.valueOf(incomingModelCodeMap.get(modelCode)) + 1;
			incomingModelCodeMap.put(modelCode, String.valueOf(count));
		}
	}
	
	public Map<String, String> getBoostFromModelCode(Map<String, String> modelCodeMap){
		Map<String, BoostBrowseBuSubBu> modelCodeToBoostBusSubBuMap = boostBrowseBuSubBuDao.getBoostBuSubBuFromModelCode();
		Map<String, String> boostValueMap = new HashMap<String, String>();
		for(String modelCode : modelCodeMap.keySet()){
			if(modelCodeToBoostBusSubBuMap.containsKey(modelCode)){
				boostValueMap.put(modelCodeToBoostBusSubBuMap.get(modelCode).getBoost(), String.valueOf(modelCodeMap.get(modelCode)));
			}
		}
		return boostValueMap;
	}
}    

