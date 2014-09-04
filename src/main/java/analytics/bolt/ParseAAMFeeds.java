package analytics.bolt;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.DBConnection;
import analytics.util.MongoUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public abstract class ParseAAMFeeds  extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected OutputCollector outputCollector;
    

	protected DB db;
	protected DBCollection memberVariablesCollection;
    protected DBCollection memberUUIDCollection;
    protected DBCollection modelVariablesCollection;


    protected List<String> modelVariablesList;
    protected Map<String,Collection<String>> l_idToValueCollectionMap; // USED TO MAP BETWEEN l_id AND THE TRAITS OR PID OR SearchKeyword ASSOCIATED WITH THAT ID UNTIL A NEW UUID IS FOUND
    protected String currentUUID;
    
    protected String topic;
	protected String sourceTopic;

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

    public void setMemberCollection(DBCollection memberCollection) {
        this.memberVariablesCollection = memberCollection;
    }
    
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */

        System.out.println("PREPARING PARSING BOLT FOR AAM Feeds");

        try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
        
        memberVariablesCollection = db.getCollection("memberVariables");
        memberUUIDCollection = db.getCollection("memberUUID");
        modelVariablesCollection = db.getCollection("modelVariables");
        modelVariablesList = new ArrayList<String>();
        
        this.currentUUID=null;
        l_idToValueCollectionMap = new HashMap<String,Collection<String>>();
        

		//POPULATE MODEL VARIABLES LIST
		DBCursor modelVariablesCursor = modelVariablesCollection.find();
		for(DBObject modelDBO:modelVariablesCursor) {
			BasicDBList variablesDBList = (BasicDBList) modelDBO.get("variable");
			for(Object var:variablesDBList) {
				if(!modelVariablesList.contains(var.toString())) {
					modelVariablesList.add(((BasicDBObject) var).get("name").toString());
				}
			}
		}
//		System.out.println(" *** PARSING BOLT MODEL VARIABLE LIST: ");
//		System.out.println(modelVariablesList);
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
		
		//System.out.println("PARSING DOCUMENT -- WEB TRAIT RECORD " + input.getString(0));
		
		// 1) SPLIT INPUT STRING
		
        String interactionRec = input.getString(1);
        String splitRecArray[] = splitRec(interactionRec);
        
        //does nothing but print out split string
        String splitRec = new String();
        for(int i=0;i<splitRecArray.length;i++) {
        	if(i==0) splitRec = splitRecArray[i];
        	else splitRec = splitRec + "  " + splitRecArray[i];
        }
//        System.out.println("  split string: " + splitRec);
		
        
        //2014-03-08 10:56:17,00000388763646853831116694914086674166,743651,US,Sears
        if(splitRecArray == null || splitRecArray.length==0) {
        	return;
        }
        
        
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
            System.out.println("processing found traits...");
            
            for(String current_l_id : l_idToValueCollectionMap.keySet()) {
	        	
	        	Map<String,String> variableValueMap = processList(current_l_id); //LIST OF VARIABLES FOUND DURING TRAITS PROCESSING
	        	if(variableValueMap !=null && !variableValueMap.isEmpty()) {
	 	        	Object variableValueJSON = createJsonFromStringStringMap(variableValueMap);
		        	List<Object> listToEmit = new ArrayList<Object>();
		        	listToEmit.add(current_l_id);
		        	listToEmit.add(variableValueJSON);
		        	listToEmit.add(sourceTopic);
		        	this.outputCollector.emit(listToEmit);
		        	System.out.println(" *** PARSING BOLT EMITTING: " + listToEmit);
	        	}
	        	else {
	           		System.out.println(" *** NO VARIABLES FOUND - NOTHING TO EMIT");
	        	}
            }
            this.currentUUID=null;
            this.l_idToValueCollectionMap=new HashMap<String, Collection<String>>();
        }
        
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
        //		If l_id is null and the next UUID is the same the current, then the next record will not be processed
        DBCursor uuidCursor = memberUUIDCollection.find(new BasicDBObject("u",splitRecArray[0]));
        if(uuidCursor == null) {
            this.currentUUID=splitRecArray[0];
            System.out.println(" *** COULD NOT FIND UUID: " + this.currentUUID);
        	this.l_idToValueCollectionMap=new HashMap<String, Collection<String>>();
        	return;
        }
        
        // set current uuid and l_id from mongoDB query results
        for(DBObject uuidDbo:uuidCursor) {
        	if(this.currentUUID == null) {
        		this.currentUUID = uuidDbo.get("u").toString();
        	}
        	try{
        	l_idToValueCollectionMap.put(uuidDbo.get("l_id").toString(), new ArrayList<String>());
        	}catch(NullPointerException e){
        		System.out.println("should not reach here");
        		System.out.println(l_idToValueCollectionMap);
        		System.out.println(uuidDbo.get("l_id").toString());
        		System.exit(0);
        	}
    		for(int i=1;i<splitRecArray.length;i++){
    			l_idToValueCollectionMap.get(uuidDbo.get("l_id")).add(splitRecArray[i].trim());
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
			e.printStackTrace();
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
	
	
	//TODO: Move this to a util class
    abstract protected String[] splitRec(String webRec);
    
	//TODO: Move this to a util class
	protected boolean hasModelVariable(Collection<String> varCollection) {
		boolean isModVar = false;
		for(String v:varCollection) {
			if(modelVariablesList.contains(v)) {
				isModVar = true;
			}
		}
		return isModVar;
	}

	
	//TODO: Move this to a util class
	protected Object createJsonFromStringStringMap(Map<String,String> variableValuesMap) {
		
		Gson gson = new Gson();		
    	Type varValueType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
    	String varValueString = gson.toJson(variableValuesMap, varValueType);
		return varValueString;
	}

	

}
