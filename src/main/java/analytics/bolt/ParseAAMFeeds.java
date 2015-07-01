package analytics.bolt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.ModelVariablesDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public abstract class ParseAAMFeeds  extends EnvironmentBolt {

	static final Logger LOGGER = LoggerFactory
			.getLogger(ParseAAMFeeds.class);
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;

    protected List<String> modelVariablesList;
    protected Map<String,Collection<String>> l_idToValueCollectionMap; // USED TO MAP BETWEEN l_id AND THE TRAITS OR PID OR SearchKeyword ASSOCIATED WITH THAT ID 
    protected String loyalty_id;
    
    protected String topic;
	protected String sourceTopic;
//	protected MemberUUIDDao memberDao;
	protected ModelVariablesDao modelVariablesDao;
	
	private static String topicsUsingEncryptLidDirectly = "SIGNAL_Feed";
			
    public ParseAAMFeeds() {
	}

	// Overloaded Paramterized constructor to get the topic to which the spout is listening to
	public ParseAAMFeeds( String systemProperty, String topic) {
		super(systemProperty);
		this.topic = topic;
		}
	
    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        super.prepare(stormConf, context, collector);
	//    memberDao = new MemberUUIDDao();
        modelVariablesDao =  new ModelVariablesDao(); 
        modelVariablesList = new ArrayList<String>();
     
		//POPULATE MODEL VARIABLES LIST
        modelVariablesList =modelVariablesDao.getVariableList();
    }

	@Override
	public void execute(Tuple input) {
		
		LOGGER.debug("PARSING DOCUMENT -- WEB TRAIT RECORD ");
		redisCountIncr("incoming_tuples");
		//countMetric.scope("incoming_tuples").incr();
		// 1) SPLIT INPUT STRING
		
        String interactionRec = input.getString(0);
        String splitRecArray[] = splitRec(interactionRec);
        
        if(splitRecArray == null || splitRecArray.length==0) {
    		//countMetric.scope("invalid_record").incr();
    		redisCountIncr("invalid_record");
    		outputCollector.ack(input);
        	return;
        }
        
      //condition to check whether loyaltyId or l_ids is passed
        String l_id = null;
        if(!topicsUsingEncryptLidDirectly.contains(topic)){
			// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
	        //		If l_id is null and the next UUID is the same the current, then the next record will not be processed
	        this.loyalty_id = splitRecArray[0].trim();
	        if(loyalty_id.length()!=16 || !loyalty_id.startsWith("7081")){
	        	LOGGER.info("Could not find Lid: " + this.loyalty_id);
	        	//countMetric.scope("no_lids").incr();
	        	redisCountIncr("no_lids");
	        	outputCollector.ack(input);
	        	return;
	        }
	        l_id = SecurityUtils.hashLoyaltyId(loyalty_id);
        
        }
        else
        	l_id = splitRecArray[0];
        
        
        l_idToValueCollectionMap = new HashMap<String, Collection<String>>();
        // set current uuid and l_id from mongoDB query results
        //for(String l_id:l_ids) {
        	l_idToValueCollectionMap.put(l_id, new ArrayList<String>());
    		for(int i=1;i<splitRecArray.length;i++){
    			l_idToValueCollectionMap.get(l_id).add(splitRecArray[i].trim());
    		}
    		LOGGER.debug("processing found traits...");
        	Map<String,String> variableValueMap = processList(l_id); //LIST OF VARIABLES FOUND DURING TRAITS PROCESSING
        	if(variableValueMap !=null && !variableValueMap.isEmpty()) {
 	        	Object variableValueJSON = JsonUtils.createJsonFromStringStringMap(variableValueMap);
	        	List<Object> listToEmit = new ArrayList<Object>();
	        	listToEmit.add(l_id);
	        	listToEmit.add(variableValueJSON);
	        	listToEmit.add(sourceTopic);
	        	listToEmit.add(loyalty_id);
	        	this.outputCollector.emit(listToEmit);
	        	//countMetric.scope("processed_lid").incr();
	        	redisCountIncr("processed_lid");
	        	LOGGER.debug(" *** PARSING BOLT EMITTING: " + listToEmit);
        	}
        	else {
        		LOGGER.debug(" *** NO VARIABLES FOUND - NOTHING TO EMIT");
        		redisCountIncr("no_variables_affected");
        		//countMetric.scope("no_variables_affected").incr();
        	}
        	//countMetric.scope("total_processing").incr();
        	redisCountIncr("total_processing");
        	
        //}
        
        //TODO: If we need this, we should ask Dustin to send it to Traits feed as well
        /*SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
        Date interactionDateTime = new Date();
        try {
			interactionDateTime = dateTimeFormat.parse(splitRecArray[0]);
		} catch (ParseException e) {
			logger.debug("Can not parse date",e);
		}*/
        
		//countMetric.scope("unique_uuids_processed").incr();
        	redisCountIncr("unique_uuids_processed");
		outputCollector.ack(input);
    	return;
        
	}

	protected abstract Map<String, String> processList(String current_l_id);

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source","lyl_id_no"));
	}
	
	
    abstract protected String[] splitRec(String webRec);
    


}
