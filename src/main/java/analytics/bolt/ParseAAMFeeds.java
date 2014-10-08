package analytics.bolt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberUUIDDao;
import analytics.util.dao.ModelVariablesDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public abstract class ParseAAMFeeds  extends BaseRichBolt {

	static final Logger LOGGER = LoggerFactory
			.getLogger(ParseAAMFeeds.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;

    protected List<String> modelVariablesList;
    protected Map<String,Collection<String>> l_idToValueCollectionMap; // USED TO MAP BETWEEN l_id AND THE TRAITS OR PID OR SearchKeyword ASSOCIATED WITH THAT ID UNTIL A NEW UUID IS FOUND
    protected String currentUUID;
    
    protected String topic;
	protected String sourceTopic;
	protected MemberUUIDDao memberDao;
	protected ModelVariablesDao modelVariablesDao;

    public ParseAAMFeeds() {

	}

	// Overloaded Paramterized constructor to get the topic to which the spout
	// is listening to
	public ParseAAMFeeds(String topic) {
		this.topic = topic;
	}
	
    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
        
	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
        memberDao = new MemberUUIDDao();
        modelVariablesDao =  new ModelVariablesDao(); 
        modelVariablesList = new ArrayList<String>();
        
        this.currentUUID=null;
        l_idToValueCollectionMap = new HashMap<String,Collection<String>>();
        

		//POPULATE MODEL VARIABLES LIST
        modelVariablesList =modelVariablesDao.getVariableList();
    }

	/*
     * (non-Javadoc)
     *
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
	@Override
	public void execute(Tuple input) {

		// 1) SPLIT STRING
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD TRAIT TO LIST AND RETURN
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT TRAITS LIST AND EMIT VARIABLES
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
		// 5) POPULATE TRAITS COLLECTION WITH THE FIRST TRAIT
		
		LOGGER.debug("PARSING DOCUMENT -- WEB TRAIT RECORD " + input.getString(0));
		
		// 1) SPLIT INPUT STRING
		
        String interactionRec = input.getString(1);
        String splitRecArray[] = splitRec(interactionRec);
        
        if(splitRecArray == null || splitRecArray.length==0) {
        	return;
        }
        
        //2014-03-08 10:56:17,00000388763646853831116694914086674166,743651,US,Sears
        
        
        
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD TRAIT TO LIST AND RETURN
        if(this.currentUUID != null && this.currentUUID.equalsIgnoreCase(splitRecArray[0])) {
        	//skip processing if l_id is null
        	if(this.l_idToValueCollectionMap==null || this.l_idToValueCollectionMap.isEmpty()) {
        		return;
        	}
        	
        	for(String l : l_idToValueCollectionMap.keySet()) {
        		for(int i=1;i<splitRecArray.length;i++){
        			l_idToValueCollectionMap.get(l).add(splitRecArray[i].trim());
        		}
        	}
        	return;
        }
        
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT VALUES(TRAIT/PID/Keyword) LIST AND EMIT VARIABLES
        if(l_idToValueCollectionMap != null && !l_idToValueCollectionMap.isEmpty()) {
            LOGGER.debug("processing found traits...");
            
            for(String current_l_id : l_idToValueCollectionMap.keySet()) {
	        	
	        	Map<String,String> variableValueMap = processList(current_l_id); //LIST OF VARIABLES FOUND DURING TRAITS PROCESSING
	        	if(variableValueMap !=null && !variableValueMap.isEmpty()) {
	 	        	Object variableValueJSON = JsonUtils.createJsonFromStringStringMap(variableValueMap);
		        	List<Object> listToEmit = new ArrayList<Object>();
		        	listToEmit.add(current_l_id);
		        	listToEmit.add(variableValueJSON);
		        	listToEmit.add(sourceTopic);
		        	this.outputCollector.emit(listToEmit);
		        	LOGGER.debug(" *** PARSING BOLT EMITTING: " + listToEmit);
	        	}
	        	else {
	        		LOGGER.debug(" *** NO VARIABLES FOUND - NOTHING TO EMIT");
	        	}
            }
            this.currentUUID=null;
            this.l_idToValueCollectionMap=new HashMap<String, Collection<String>>();
        }
        
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
        //		If l_id is null and the next UUID is the same the current, then the next record will not be processed
        List<String> l_ids = memberDao.getLoyaltyIdsFromUUID(splitRecArray[0]);
        if(l_ids == null || l_ids.size() == 0) {
            this.currentUUID=splitRecArray[0];
            LOGGER.info(" *** COULD NOT FIND UUID: " + this.currentUUID);
        	this.l_idToValueCollectionMap=new HashMap<String, Collection<String>>();
        	return;
        }
        
        // set current uuid and l_id from mongoDB query results
        for(String l_id:l_ids) {
        	if(this.currentUUID == null) {
        		this.currentUUID = l_id;
        	}
        	try{
        	l_idToValueCollectionMap.put(l_id, new ArrayList<String>());
        	}catch(NullPointerException e){
        		LOGGER.error("l_id to value collection is null", e);
        		System.exit(0);
        	}
    		for(int i=1;i<splitRecArray.length;i++){
    			l_idToValueCollectionMap.get(l_id).add(splitRecArray[i].trim());
    		}
        	
        }
        
        if(l_idToValueCollectionMap == null || l_idToValueCollectionMap.isEmpty()) {
        	this.l_idToValueCollectionMap=new HashMap<String, Collection<String>>();
        	return;
        }
        
        //TODO: If we need this, we should ask Dustin to send it to Traits feed as well
        /*SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
        Date interactionDateTime = new Date();
        try {
			interactionDateTime = dateTimeFormat.parse(splitRecArray[0]);
		} catch (ParseException e) {
			logger.debug("Can not parse date",e);
		}*/
        
        return;
        
	}


	protected abstract Map<String, String> processList(String current_l_id);

	/*
     * (non-Javadoc)
     *
     * @see
     * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
     * topology.OutputFieldsDeclarer)
     */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source"));
	}
	
	
    abstract protected String[] splitRec(String webRec);
    


}
