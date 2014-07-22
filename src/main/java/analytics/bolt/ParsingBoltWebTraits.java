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


public class ParsingBoltWebTraits extends BaseRichBolt {
	/**
	 * Created by Rock Wasserman 4/18/2014
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    

    DB db;
    MongoClient mongoClient;
    DBCollection memberCollection;
    DBCollection memberTraitsCollection;
    DBCollection memberUUIDCollection;
    DBCollection traitVariablesCollection;

    private Map<String,Collection<String>> traitVariablesMap;
    private Map<String,Collection<String>> variableTraitsMap;
    private Map<String,Map<String,String>> memberTraitsMap; // MAP BETWEEN l_id AND SET OF TRAITS - HISTORICAL AND CURRENT TRAITS
    private Map<String,Collection<String>> l_idToTraitCollectionMap; // USED TO MAP BETWEEN l_id AND THE TRAITS ASSOCIATED WITH THAT ID UNTIL A NEW UUID IS FOUND
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

        System.out.println("PREPARING PARSING BOLT FOR WEB TRAITS");
        try {
//            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
            mongoClient = new MongoClient("trprrta2mong4.vm.itg.corp.us.shldcorp.com", 27000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

//		db = mongoClient.getDB("RealTimeScoring");
//	    db.authenticate("rtsw", "5core123".toCharArray());
        db = mongoClient.getDB("test");
        memberCollection = db.getCollection("memberVariables");
        memberTraitsCollection = db.getCollection("memberTraits");
        memberUUIDCollection = db.getCollection("memberUUID");
        traitVariablesCollection = db.getCollection("traitVariables");
        
        this.currentUUID=null;
        this.current_l_id=null;
        l_idToTraitCollectionMap = new HashMap<String,Collection<String>>();
        
        
        // POPULATE THE TRAIT TO VARIABLES MAP AND THE VARIABLE TO TRAITS MAP
        
        traitVariablesMap = new HashMap<String, Collection<String>>();
        variableTraitsMap = new HashMap<String, Collection<String>>();
		DBCursor traitVarCursor = traitVariablesCollection.find();
		
		for(DBObject traitVariablesDBO: traitVarCursor) {
			BasicDBList variables = (BasicDBList) traitVariablesDBO.get("v");
			for(Object v:variables) {
				String variable = (String) v;
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
//		System.out.println("*** TRAIT TO VARIABLES MAP >>>");
//		System.out.println(traitVariablesMap);
		
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
        String webTraitInteractionRec = input.getString(1);
        String webTraitsSplitRec[] = splitRec(webTraitInteractionRec);
        
//        String splitRec = new String();
//        for(int i=0;i<webTraitsSplitRec.length;i++) {
//        	if(i==0) splitRec = webTraitsSplitRec[i];
//        	else splitRec = splitRec + "  " + webTraitsSplitRec[i];
//        }
//        System.out.println("  split string: " + splitRec);
		
        
        //2014-03-08 10:56:17,00000388763646853831116694914086674166,743651,US,Sears
        if(webTraitsSplitRec == null || webTraitsSplitRec.length==0) {
        	return;
        }
        
        
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD TRAIT TO LIST AND RETURN
        if(this.currentUUID !=null && this.currentUUID.equalsIgnoreCase(webTraitsSplitRec[1])) {
        	//skip processing if l_id is null
        	if(this.current_l_id == null) {
        		//System.out.println(" NULL l_id -- return");
        		return;
        	}
        	
        	Collection<String> traits = l_idToTraitCollectionMap.get(current_l_id);
			if (traits != null) {
				traits.add(webTraitsSplitRec[2]);
			}
            //System.out.println(" *** ADDING WEB TRAIT: " + webTraitsSplitRec[2]);
        	return;
        }
        
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT TRAITS LIST AND EMIT VARIABLES
        if(l_idToTraitCollectionMap != null && !l_idToTraitCollectionMap.isEmpty()) {
        	Map<String,String> variableValueMap = processTraitsList();
        	if(variableValueMap!=null && !variableValueMap.isEmpty()) {
 	        	Object variableValueJSON = createJsonFromVariableValueMap(variableValueMap);
	        	List<Object> listToEmit = new ArrayList<Object>();
	        	listToEmit.add(current_l_id);
	        	listToEmit.add(variableValueJSON);
	        	listToEmit.add("WebTraits");
	        	this.outputCollector.emit(listToEmit);
	        	System.out.println(" *** PARSING BOLT EMITTING: " + listToEmit);
        	}
//        	else {
//           		System.out.println(" *** NO VARIBALES FOUND - NOTHING TO EMIT");
//        	}
        	l_idToTraitCollectionMap.remove(current_l_id);
            this.currentUUID=null;
            this.current_l_id=null;
        }
        
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
        //		If l_id is null and the next UUID is the same the current, then the next record will not be processed
        DBObject uuid = memberUUIDCollection.findOne(new BasicDBObject("u",webTraitsSplitRec[1]));
        if(uuid == null) {
            //System.out.println(" *** COULD NOT FIND UUID");
            this.currentUUID=webTraitsSplitRec[1];
        	this.current_l_id=null;
        	return;
        }
        
        // set current uuid and l_id from mongoDB query results
        this.currentUUID = (String) uuid.get("u");
        String l_id = (String) uuid.get("l_id");
        
        if(l_id == null || l_id == "") {
        	this.current_l_id=null;
        	return;
        }
        
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");
        Date interactionDateTime = new Date();
        try {
			interactionDateTime = dateTimeFormat.parse(webTraitsSplitRec[0]);
		} catch (ParseException e) {
			e.printStackTrace();
		}
        
        //System.out.println(" *** FOUND l_id: " + l_id + " interaction time: " + interactionDateTime + " trait: " + webTraitsSplitRec[2]);
        this.current_l_id = l_id;
        

		// 5) POPULATE TRAITS COLLECTION WITH THE FIRST TRAIT
        Collection<String> firstTrait = new ArrayList<String>();
        firstTrait.add(webTraitsSplitRec[2]);
        
        //System.out.println(" *** PUT IN FIRST RECORD: " + this.current_l_id + " trait: " + firstTrait);
        l_idToTraitCollectionMap.put(this.current_l_id,firstTrait);
        
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

    private Map<String,String> processTraitsList() {
    	Map<String,String> variableValueMap = new HashMap<String,String>();
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	boolean firstTrait = true; //flag to indicate if the AMM trait found is the first for that member - if true then populate the memberTraitsMap
    	
    	//FOR EACH TRAIT FOUND FROM AAM DATA FIND THE VARIABLES THAT ARE IMPACTED
    	for(String trait: l_idToTraitCollectionMap.get(this.current_l_id)) {
    		if(traitVariablesMap.containsKey(trait)) {
    			if(firstTrait) {
    				prepareMemberTraitsMap();
    				firstTrait = false;
    			}
    			Collection<String> varCollection = traitVariablesMap.get(trait);
    			for(String variable:varCollection) {
	    			if(variableValueMap.containsKey(variable)) {
	    				int value = 1 + Integer.valueOf(variableValueMap.get(variable));
	    				variableValueMap.remove(variable);
	    				variableValueMap.put(variable, String.valueOf(value));
	    			}
	    			else {
	    				variableValueMap.put(variable, "1");
	    				//System.out.println(" *** FOUND VARIABLE: " + variable);
	    			}
	    			
	    			if(memberTraitsMap.containsKey(this.current_l_id)) {
		    			if(memberTraitsMap.get(this.current_l_id).containsKey(trait)) {
		    				memberTraitsMap.get(this.current_l_id).remove(trait);
		    			}
	    				memberTraitsMap.get(this.current_l_id).put(trait,simpleDateFormat.format(new Date()));
	    			}
	    			else {
	    				// IF MEMBER NOT FOUND IN QUERY, ADD MEMBER TO TRAIT MAP - NO HISTORY
	    				memberTraitsMap.put(this.current_l_id, new HashMap<String, String>());
	    				memberTraitsMap.get(this.current_l_id).put(trait,simpleDateFormat.format(new Date()));
	    			}
    			}
    		}
    	}
    	
    	//FOR EACH VARIABLE FOUND FIND ALL TRAITS IN MEMBER TO TRAITS MAP ASSOCIATED WITH THOSE VARIABLES, COUNT TRAITS AND RETURN
    	if(variableValueMap != null && !variableValueMap.isEmpty()) {
	    	System.out.println(" *** VARIABLE-VALUE MAP: " + variableValueMap);
    		int uniqueTraitCount = 0;
    		Map<String,String> variableUniqueCountMap = new HashMap<String,String>();
    		for(String variable:variableValueMap.keySet()) {
    			for(String trait: variableTraitsMap.get(variable)) {
    				if(memberTraitsMap.get(this.current_l_id).containsKey(trait)) {
    					boolean checkDate = false;
    					try {
    						//TODO change to current date after testing is completed
    						checkDate = simpleDateFormat.parse(memberTraitsMap.get(this.current_l_id).get(trait)).after(simpleDateFormat.parse("2014-07-14") /*format(new Date().getTime() + (-7 * 1000 * 60 * 60 * 24)*/);
						} catch (ParseException e) {
							e.printStackTrace();
						}
    					if(checkDate) {
    						uniqueTraitCount++;
    					}
    				}
    			}
    			if(uniqueTraitCount>0) {
    				variableUniqueCountMap.put(variable, String.valueOf(uniqueTraitCount));
    			}
    		}
    		if(variableUniqueCountMap!=null && !variableUniqueCountMap.isEmpty()) {
        		for(String t: variableUniqueCountMap.keySet()) {
        			System.out.println(" *** UNIQUE TRAITS COUNT FOR VARIABLE [" + t + "]: " + variableUniqueCountMap.get(t));
        		}
    			return variableUniqueCountMap;
    		}
    	}
    	else {
    		System.out.println(" *** NO TRAITS FOUND TO RESCORE");
    	}
    	return null;
    }
    
    
    private void prepareMemberTraitsMap() {
		memberTraitsMap = new HashMap<String,Map<String,String>>();
		BasicDBObject queryMemberTraitsCollection = new BasicDBObject().append("l_id", this.current_l_id);
		DBObject memberTraitsDBO = memberTraitsCollection.findOne(queryMemberTraitsCollection);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		if(memberTraitsDBO != null && memberTraitsDBO.keySet().contains(this.current_l_id)) {
			Map<String,String> traitsMap = new HashMap<String,String>();
			memberTraitsMap.put(this.current_l_id, traitsMap);
			
			BasicDBList traits = (BasicDBList) memberTraitsDBO.get("traits");
			
			for( Iterator<Object> it = traits.iterator(); it.hasNext(); ) {
				BasicDBObject trait = (BasicDBObject) it.next();
				try {
					if(!simpleDateFormat.parse(trait.get("d").toString()).after(new Date())) {
						memberTraitsMap.get(this.current_l_id).put(trait.get("t").toString(), trait.get("d").toString());
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
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
				e.printStackTrace();
			}
			byte[] rawHmac = mac.doFinal(l_id.getBytes());
			hashed = new String(Base64.encodeBase64(rawHmac));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return hashed;
	}
}
