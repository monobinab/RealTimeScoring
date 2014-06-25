package analytics.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class ParsingBoltAAM_ATC extends BaseRichBolt {

	/**
	 * Created by Rock Wasserman 6/19/2014
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    

    DB db;
    MongoClient mongoClient;
    DBCollection memberCollection;
    DBCollection memberUUIDCollection;
    DBCollection pidDivLnCollection;
    DBCollection divLnVariableCollection;

    private Map<String,Collection<String>> divLnVariablesMap;
    private Map<String,Collection<String>> l_idToPidCollectionMap; // USED TO MAP BETWEEN l_id AND THE DIVISION AND LINE ASSOCIATED WITH THAT ID UNTIL A NEW UUID IS FOUND
    private String currentUUID;
    private String current_l_id;


    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public void setMemberCollection(DBCollection memberCollection) {
        this.memberCollection = memberCollection;
    }

//    public void setDivLnItmCollection(DBCollection divLnItmCollection) {
//        this.divLnItmCollection = divLnItmCollection;
//    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */

        //System.out.println("PREPARING PARSING BOLT FOR WEB TRAITS");
        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
	    db.authenticate("rtsw", "5core123".toCharArray());
        memberCollection = db.getCollection("memberVariables");
        memberUUIDCollection = db.getCollection("memberUUID");
        
        this.currentUUID=null;
        this.current_l_id=null;
        l_idToPidCollectionMap = new HashMap<String,Collection<String>>();
        
        pidDivLnCollection = db.getCollection("pidDivLn");
        divLnVariableCollection = db.getCollection("divLnVariable");

        
        // populate divLnVariablesMap
        divLnVariablesMap = new HashMap<String, Collection<String>>();
        DBCursor divLnVarCursor = divLnVariableCollection.find();
        for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnVariablesMap.get(divLnDBObject.get("d")) == null)
            {
                Collection<String> varColl = new ArrayList<String>();
                varColl.add(divLnDBObject.get("v").toString());
                divLnVariablesMap.put(divLnDBObject.get("d").toString(), varColl);
            }
            else
            {
                Collection<String> varColl = divLnVariablesMap.get(divLnDBObject.get("d").toString());
                varColl.add(divLnDBObject.get("v").toString().toUpperCase());
                divLnVariablesMap.put(divLnDBObject.get("d").toString(), varColl);
            }
        }
    }


    /*
     * (non-Javadoc)
     *
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
	@Override
	public void execute(Tuple input) {

		// 1) SPLIT STRING
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD PID TO LIST AND RETURN
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT PIDs LIST AND EMIT VARIABLES
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
		// 5) POPULATE PID COLLECTION WITH THE FIRST PID
		
		
		
		System.out.println("PARSING DOCUMENT -- ATC RECORD " + input.getString(0));
		
		// 1) SPLIT INPUT STRING
        String pidInteractionRec = input.getString(1);
        String pidSplitRec[] = splitRec(pidInteractionRec);

        //NOT USED FOR ANYTING OTHER THAN TO CHECK HOW THE RECORD WAS SPLIT
//        String splitRec = new String();
//        for(int i=0; i < PidSplitRec.length; i++) {
//        	if(i==0) splitRec = PidSplitRec[i];
//        	else splitRec = splitRec + " ~~~ " + PidSplitRec[i];
//        }
//        System.out.println("  split string: " + splitRec);
		
        
        if(pidSplitRec == null || pidSplitRec.length==0) {
        	return;
        }
        
        
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD PID TO LIST AND RETURN
        if(this.currentUUID !=null && this.currentUUID.equalsIgnoreCase(pidSplitRec[1])) {
        	//do not process if current_l_id is null
        	if(this.current_l_id == null) {
        		System.out.println(" @@@ NULL l_id -- return");
        		return;
        	}
        	
        	Collection<String> pids = l_idToPidCollectionMap.get(current_l_id);
			if (pids != null) {
				pids.add(pidSplitRec[2]);
			}
            System.out.println(" @@@ ADDING WEB TRAIT: " + pidSplitRec[2]);
        	return;
        }
        
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT PIDs LIST AND EMIT VARIABLES
        if(l_idToPidCollectionMap != null && !l_idToPidCollectionMap.isEmpty()) {
        	Map<String,String> variableValueMap = processPidList();
        	if(variableValueMap==null || variableValueMap.isEmpty()) {
        		System.out.println(" @@@ NO VARIBALES FOUND - NOTHING TO EMIT");
        		l_idToPidCollectionMap.remove(current_l_id);
        		return;
        	}
        	Object variableValueJSON = createJsonFromVariableValueMap(variableValueMap);
        	List<Object> listToEmit = new ArrayList<Object>();
        	listToEmit.add(current_l_id);
        	listToEmit.add(variableValueJSON);
        	listToEmit.add("AAM_ATC");
        	l_idToPidCollectionMap.remove(current_l_id);
            this.currentUUID=null;
            this.current_l_id=null;
        	System.out.println(" @@@ PARSING BOLT EMITTING: " + listToEmit);
        	this.outputCollector.emit(listToEmit);
        }
        
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
        DBObject uuid = memberUUIDCollection.findOne(new BasicDBObject("u",pidSplitRec[1]));
        if(uuid == null) {
            System.out.println(" @@@ COULD NOT FIND UUID");
            this.currentUUID=pidSplitRec[1];
        	this.current_l_id=null;
        	return;
        }
        
        this.currentUUID = (String) uuid.get("u");
        String l_id = (String) uuid.get("l_id");
        
        if(l_id == null || l_id == "") {
        	this.current_l_id=null;
        	return;
        }
        
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
        Date interactionDateTime = null;
        try {
			interactionDateTime = dateTimeFormat.parse(pidSplitRec[0]);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        System.out.println(" @@@ FOUND l_id: " + l_id + " interaction time: " + interactionDateTime + " trait: " + pidSplitRec[2]);
        this.current_l_id = l_id;
        

		// 5) POPULATE PID COLLECTION WITH THE FIRST PID
        Collection<String> firstPid = new ArrayList<String>();
        firstPid.add(pidSplitRec[2]);
        
        //System.out.println(" @@@ PUT IN FIRST RECORD: " + this.current_l_id + " trait: " + firstTrait);
        l_idToPidCollectionMap.put(this.current_l_id,firstPid);
        
        return;
        
	}

    private String[] splitRec(String webRec) {
        //System.out.println("WEB RECORD: " + webRec);
        String split[]=StringUtils.split(webRec,",");
        
        if(split !=null && split.length>0) {
			return split;
		}
		else {
			return null;
		}
	}

    private Map<String,String> processPidList() {
    	Map<String,String> variableValueMap = new HashMap<String,String>();
    	
    	for(String pid: l_idToPidCollectionMap.get(current_l_id)) {
    		//query MongoDB for division and line associated with the pid
    		DBObject pidDivLnQueryResult = pidDivLnCollection.findOne(new BasicDBObject().append("pid", pid));
    		if(pidDivLnQueryResult != null) {
    			//get division and division/line concatenation from query results
	    		String div = pidDivLnQueryResult.get("d").toString();
	    		String divLn = pidDivLnQueryResult.get("l").toString();
	    		Collection<String> var = new ArrayList<String>();
	    		if(divLnVariablesMap.containsKey(div)) {
	    			var = divLnVariablesMap.get(div);
	    			for(String v:var) {
		    			if(variableValueMap.containsKey(var)) {
		    				int value = 1 + Integer.valueOf(variableValueMap.get(v));
		    				variableValueMap.remove(v);
		    				variableValueMap.put(v, String.valueOf(value));
		    			}
		    			else {
		    				variableValueMap.put(v, "1");
		    			}
	    			}
	    		}
	    		if(divLnVariablesMap.containsKey(divLn)) {
	    			var = divLnVariablesMap.get(divLn);
	    			for(String v:var) {
		    			if(variableValueMap.containsKey(var)) {
		    				int value = 1 + Integer.valueOf(variableValueMap.get(v));
		    				variableValueMap.remove(v);
		    				variableValueMap.put(v, String.valueOf(value));
		    			}
		    			else {
		    				variableValueMap.put(v, "1");
		    			}
	    			}
	    		}
    		}
    	}
    	return variableValueMap;
    }
    
    
	private Object createJsonFromVariableValueMap(Map<String, String> variableValueMap) {
		// Create string in JSON format to emit
    	Gson gson = new Gson();
    	Type varValueType = new TypeToken<Map<String, String>>() {}.getType();
    	String varValueString = gson.toJson(variableValueMap, varValueType);
    	
		return varValueString;
	}

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


	public String hashLoyaltyId(String l_id) {
		String hashed = new String();
		try {
			SecretKeySpec signingKey = new SecretKeySpec("mykey".getBytes(), "HmacSHA1");
			Mac mac = Mac.getInstance("HmacSHA1");
			try {
				mac.init(signingKey);
			} catch (InvalidKeyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			byte[] rawHmac = mac.doFinal(l_id.getBytes());
			hashed = new String(Base64.encodeBase64(rawHmac));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hashed;
	}
	
//	public String getLineFromCollection(String div, String item) {
//		//System.out.println("searching for line");
//		
//		BasicDBObject queryLine = new BasicDBObject();
//		queryLine.put("d", div);
//		queryLine.put("i", item);
//		
//		DBObject divLnItm = divLnItmCollection.findOne(queryLine);
//		
//		if(divLnItm==null || divLnItm.keySet()==null || divLnItm.keySet().isEmpty()) {
//			return null;
//		}
//		String line = divLnItm.get("l").toString();
//		//System.out.println("  found line: " + line);
//		return line;
//	}
	

}
