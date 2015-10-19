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
    
    protected String source;
    protected String sourceTopic;
	protected ModelVariablesDao modelVariablesDao;
			
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
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        super.prepare(stormConf, context, collector);
	    modelVariablesDao =  new ModelVariablesDao(); 
        modelVariablesList = new ArrayList<String>();
     
		//POPULATE MODEL VARIABLES LIST
        modelVariablesList = modelVariablesDao.getVariableList();
    }

	@Override
	public void execute(Tuple input) {
		
		String loyalty_id;
		
		LOGGER.debug("PARSING DOCUMENT -- WEB TRAIT RECORD ");
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
        
         l_idToValueCollectionMap = new HashMap<String, Collection<String>>();
         l_idToValueCollectionMap.put(l_id, new ArrayList<String>());
		for(int i=1;i<splitRecArray.length;i++){
			l_idToValueCollectionMap.get(l_id).add(splitRecArray[i].trim());
		}
		LOGGER.debug("processing found traits...");
		
		Map<String, Integer> tagsMap = new HashMap<String, Integer>();
    	Map<String,String> variableValueMap = processList(l_id, tagsMap); //LIST OF VARIABLES FOUND DURING TRAITS PROCESSING
    	if(variableValueMap !=null && !variableValueMap.isEmpty()) {
        	Object variableValueJSON = JsonUtils.createJsonFromStringStringMap(variableValueMap);
        	List<Object> listToEmit = new ArrayList<Object>();
        	listToEmit.add(l_id);
        	listToEmit.add(variableValueJSON);
        	listToEmit.add(source);
        	listToEmit.add(loyalty_id);
        	this.outputCollector.emit(listToEmit);
        	redisCountIncr("processed_lid");
        	LOGGER.debug(" *** PARSING BOLT EMITTING: " + listToEmit);
    	}
    	else {
    		LOGGER.debug(" *** NO VARIABLES FOUND - NOTHING TO EMIT");
    		redisCountIncr("no_variables_affected");
    	}
    	redisCountIncr("total_processing");
    	
    	//emitting to BrowseCountPersistBolt
    	if(!tagsMap.isEmpty()){
    		Object tagsJSON = JsonUtils.createJsonFromStringIntMap(tagsMap);
    		List<Object> listToEmit = new ArrayList<Object>();
        	listToEmit.add(loyalty_id);
        	listToEmit.add(tagsJSON);
        	listToEmit.add(source);
        	this.outputCollector.emit("browse_tag_stream", listToEmit);
        	redisCountIncr("emitted_browse_tag");
    	}
    	outputCollector.ack(input);
    	return;
        
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source","lyl_id_no"));
		declarer.declareStream("browse_tag_stream", new Fields("loyalty_id","tagsJSON","source"));
	}
	
    abstract protected String[] splitRec(String webRec);
	protected abstract Map<String, String> processList(String current_l_id, Map<String, Integer> tagsMap);

}    

