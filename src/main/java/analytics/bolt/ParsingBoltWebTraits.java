package analytics.bolt;

import analytics.util.TransactionLineItem;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.ibm.jms.JMSMessage;
import com.mongodb.*;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;


public class ParsingBoltWebTraits extends BaseRichBolt {
	/**
	 * Created by Rock Wasserman 4/18/2014
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    
    final String host;
    final int port;

    DB db;
    MongoClient mongoClient;
    DBCollection memberCollection;
    DBCollection memberUUIDCollection;
    DBCollection traitVariablesCollection;
    //TODO: not sure if these are needed
//    DBCollection divLnItmCollection;
//    DBCollection divLnVariableCollection;
    private Map<String,String> traitVariablesMap;

//    private Map<String,Collection<String>> divLnVariablesMap;
    private Map<String,Collection<String>> l_idToTraitCollectionMap; // USED TO MAP BETWEEN l_id AND THE TRAITS ASSOCIATED WITH THAT ID UNTIL A NEW UUID IS FOUND
    private String currentUUID;
    private String new_l_id;
    private String current_l_id;
    
    public ParsingBoltWebTraits(String host, int port) {
        this.host = host;
        this.port = port;
    }

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

        System.out.println("PREPARING PARSING BOLT FOR WEB TRAITS");
        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
	    db.authenticate("rtsw", "5core123".toCharArray());
        memberCollection = db.getCollection("memberVariables");
        memberUUIDCollection = db.getCollection("memberUUID");
        traitVariablesCollection = db.getCollection("traitVariables");
        
        this.currentUUID=null;
        this.current_l_id=null;
        this.new_l_id=null;
        l_idToTraitCollectionMap = new HashMap<String,Collection<String>>();
        
        // TODO: NOT SURE IF THESE ARE NEEDED
//        divLnItmCollection = db.getCollection("divLnItm");
//        divLnVariableCollection = db.getCollection("divLnVariable");

        traitVariablesMap = new HashMap<String, String>();
		DBCursor traitVarCursor = traitVariablesCollection.find();
		for(DBObject traitDBObject: traitVarCursor) {
		    if (traitVariablesMap.get(traitDBObject.get("v")) == null)
			{
		    	continue;			
			}
			else
			{
				traitVariablesMap.put(traitDBObject.get("t").toString().toUpperCase(),traitDBObject.get("v").toString());
		    }
		}
        
        // populate divLnVariablesMap
        // TODO: NOT SURE IF THIS IS NEEDED        
//        divLnVariablesMap = new HashMap<String, Collection<String>>();
//        DBCursor divLnVarCursor = divLnVariableCollection.find();
//        for(DBObject divLnDBObject: divLnVarCursor) {
//            if (divLnVariablesMap.get(divLnDBObject.get("d")) == null)
//            {
//                Collection<String> varColl = new ArrayList<String>();
//                varColl.add(divLnDBObject.get("v").toString());
//                divLnVariablesMap.put(divLnDBObject.get("d").toString(), varColl);
//            }
//            else
//            {
//                Collection<String> varColl = divLnVariablesMap.get(divLnDBObject.get("d").toString());
//                varColl.add(divLnDBObject.get("v").toString().toUpperCase());
//                divLnVariablesMap.put(divLnDBObject.get("d").toString(), varColl);
//            }
//        }
    }


    /*
     * (non-Javadoc)
     *
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
	@Override
	public void execute(Tuple input) {

		// 1) SPLIT STRIN
		// 2) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN RETURN
		// 3) POPULATE TRAITS COLLECTION UNTIL A DIFFERENT UUID IS FOUND
		// 4) 
		// 5) 
		// 6) 
		// 7) 
		
		
		
		//System.out.println("PARSING DOCUMENT -- WEB TRAIT RECORD");
		
		// 1) SPLIT INPUT STRING
        String webTraitInteractionRec = input.getString(0);
        String webTraitsSplitRec[] = splitRec(webTraitInteractionRec);
        
        String splitRec = new String();
        for(int i=0;i<webTraitsSplitRec.length;i++) {
        	if(i==0) splitRec = webTraitsSplitRec[i];
        	else splitRec = splitRec + "  " + webTraitsSplitRec[i];
        }
        System.out.println("  split string: " + splitRec);
		
        
        //2014-03-08 10:56:17,00000388763646853831116694914086674166,743651,US,Sears
        if(webTraitsSplitRec == null || webTraitsSplitRec.length==0) {
        	return;
        }
        
        
        if(this.currentUUID !=null && this.currentUUID.equalsIgnoreCase(webTraitsSplitRec[1])) {
        	if(this.current_l_id == null) {
        		//System.out.println(" NULL l_id -- return");
        		return;
        	}
        	l_idToTraitCollectionMap.get(current_l_id).add(webTraitsSplitRec[2]);
            System.out.println(" *** ADDING WEB TRAIT: " + webTraitsSplitRec[2]);
        	return;
        }
        
        if(l_idToTraitCollectionMap != null && !l_idToTraitCollectionMap.isEmpty()) {
        	Map<String,String> variableValueMap = processTraitsList();
        	if(variableValueMap==null || variableValueMap.isEmpty()) {
        		System.out.println(" *** NO VARIBALES FOUND - NOTHING TO EMIT");
            	l_idToTraitCollectionMap.remove(current_l_id);
        		return;
        	}
        	Object variableValueJSON = createJsonFromVariableValueMap(variableValueMap);
        	List<Object> listToEmit = new ArrayList<Object>();
        	listToEmit.add(current_l_id);
        	listToEmit.add(variableValueJSON);
        	l_idToTraitCollectionMap.remove(current_l_id);
            this.currentUUID=null;
            this.current_l_id=null;
        	System.out.println(" *** PARSING BOLT EMITTING: " + listToEmit);
        	this.outputCollector.emit(listToEmit);
        }
        
		// 2) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN RETURN
        DBObject uuid = memberUUIDCollection.findOne(new BasicDBObject("u",webTraitsSplitRec[1]));
        if(uuid == null) {
            System.out.println(" *** COULD NOT FIND UUID");
            this.currentUUID=webTraitsSplitRec[1];
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
			interactionDateTime = dateTimeFormat.parse(webTraitsSplitRec[0]);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        System.out.println(" *** FOUND l_id: " + l_id + " interaction time: " + interactionDateTime + " trait: " + webTraitsSplitRec[2]);
        this.current_l_id = l_id;
        
        Collection<String> firstTrait = new ArrayList<String>();
        firstTrait.add(webTraitsSplitRec[2]);
        
        System.out.println(" *** PUT IN FIRST RECORD: " + this.current_l_id + " trait: " + firstTrait);
        
        l_idToTraitCollectionMap.put(this.current_l_id,firstTrait);
        
        return;
        
	}

    private String[] splitRec(String webRec) {
        System.out.println("WEB RECORD: " + webRec);
        String split[]=StringUtils.split(webRec,",");
        
        if(split !=null && split.length>0) {
			return split;
		}
		else {
			return null;
		}
	}

    private Map<String,String> processTraitsList() {
    	Map<String,String> variableValueMap = new HashMap<String,String>();
    	
    	for(String trait: l_idToTraitCollectionMap.get(current_l_id)) {
    		if(traitVariablesMap.containsKey(trait)) {
    			String var = traitVariablesMap.get(trait);
    			if(variableValueMap.containsKey(var)) {
    				int value = 1 + Integer.valueOf(variableValueMap.get(var));
    				variableValueMap.remove(var);
    				variableValueMap.put(var, String.valueOf(value));
    			}
    			else {
    				variableValueMap.put(var, "1");
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
		declarer.declare(new Fields("l_id","lineItemAsJsonString"));
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
