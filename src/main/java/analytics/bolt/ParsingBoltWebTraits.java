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
    DBCollection modelVariablesCollection;

    private Map<String,Collection<String>> traitVariablesMap;
    private Map<String,Collection<String>> variableTraitsMap;
    private List<String> modelVariablesList;
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
        modelVariablesCollection = db.getCollection("modelVariables");
        modelVariablesList = new ArrayList<String>();
        
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
				String variable = v.toString();
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
		
		//POPULATE MODEL VARIABLES LIST
		DBCursor modelVaribalesCursor = modelVariablesCollection.find();
		for(DBObject modelDBO:modelVaribalesCursor) {
			BasicDBList variablesDBList = (BasicDBList) modelDBO.get("variable");
			for(Object var:variablesDBList) {
				if(!modelVariablesList.contains(var.toString())) {
					modelVariablesList.add(var.toString());
				}
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
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD TRAIT TO LIST AND RETURN
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT TRAITS LIST AND EMIT VARIABLES
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
		// 5) POPULATE TRAITS COLLECTION WITH THE FIRST TRAIT
		
		
		
		//System.out.println("PARSING DOCUMENT -- WEB TRAIT RECORD " + input.getString(0));
		
		// 1) SPLIT INPUT STRING
        String webTraitInteractionRec = input.getString(1);
        String webTraitsSplitRec[] = splitRec(webTraitInteractionRec);
        
        //does nothing but print out split string
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
            Map<String, Collection<String>> dateTraitsMap  = new HashMap<String,Collection<String>>(); // MAP BETWEEN DATES AND SET OF TRAITS - HISTORICAL AND CURRENT TRAITS
        	List<String> foundVariables = processTraitsList(dateTraitsMap); //LIST OF VARIABLES FOUND DURING TRAITS PROCESSING
        	if(dateTraitsMap !=null && !dateTraitsMap.isEmpty() && foundVariables != null && !foundVariables.isEmpty()) {
 	        	Object variableValueJSON = createJsonFromMemberTraitsMap(foundVariables, dateTraitsMap);
	        	List<Object> listToEmit = new ArrayList<Object>();
	        	listToEmit.add(current_l_id);
	        	listToEmit.add(variableValueJSON);
	        	listToEmit.add("WebTraits");
	        	this.outputCollector.emit(listToEmit);
	        	System.out.println(" *** PARSING BOLT EMITTING: " + listToEmit);
        	}
        	else {
           		System.out.println(" *** NO VARIBALES FOUND - NOTHING TO EMIT");
        	}
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

    private List<String> processTraitsList(Map<String, Collection<String>> dateTraitsMap) {
		List<String> variableList = new ArrayList<String>();
    	boolean firstTrait = true; //flag to indicate if the AMM trait found is the first for that member - if true then populate the memberTraitsMap
    	boolean newTrait = false;
    	int traitCount = 0;
    	int variableCount = 0;
    	
    	//FOR EACH TRAIT FOUND FROM AAM DATA FIND THE VARIABLES THAT ARE IMPACTED
    	for(String trait: l_idToTraitCollectionMap.get(this.current_l_id)) {
    		if(traitVariablesMap.containsKey(trait) && hasModelVariable(traitVariablesMap.get(trait))) {
    			if(firstTrait) {
    				prepareDateTraitsMap(dateTraitsMap);
    				firstTrait = false;
    			}
				newTrait = addTraitToDateTraitMap(trait, dateTraitsMap);
    			
				if(newTrait) {
					traitCount++;
	    			for(String variable: traitVariablesMap.get(trait)) {
		    			if(modelVariablesList.contains(variable) && !variableList.contains(variable)) {
		    				variableList.add(variable);
		    				variableCount++;
		    			}
	    			}
	    		}
    		}
    	}
		System.out.println(" traits found: " + traitCount + " ... variables found: " + variableCount);
    	if(dateTraitsMap != null && !dateTraitsMap.isEmpty() && !variableList.isEmpty()) {
    		return variableList;
    	}
    	else {
    		return null;
    	}
    }

	private boolean hasModelVariable(Collection<String> varCollection) {
		boolean isModVar = false;
		for(String v:varCollection) {
			if(modelVariablesList.contains(v)) {
				isModVar = true;
			}
		}
		return isModVar;
	}

	private boolean addTraitToDateTraitMap(String trait, Map<String, Collection<String>> dateTraitsMap) {
		boolean addedTrait = false;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		if(!dateTraitsMap.containsKey(simpleDateFormat.format(new Date()))) {
			dateTraitsMap.put(simpleDateFormat.format(new Date()), new ArrayList<String>());
			dateTraitsMap.get(simpleDateFormat.format(new Date())).add(trait);
			addedTrait=true;
		}
		else if(!dateTraitsMap.get(simpleDateFormat.format(new Date())).contains(trait)) {
			dateTraitsMap.get(simpleDateFormat.format(new Date())).add(trait);
			addedTrait=true;
		}
		return addedTrait;
	}
    
    
    private void prepareDateTraitsMap(Map<String, Collection<String>> dateTraitsMap) {
		DBObject memberTraitsDBO = memberTraitsCollection.findOne(new BasicDBObject().append("l_id", this.current_l_id));
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		if(memberTraitsDBO != null && memberTraitsDBO.keySet().contains(this.current_l_id)) {
			
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
    
    
	private Object createJsonFromMemberTraitsMap(List<String> variableList, Map<String, Collection<String>> dateTraitsMap) {
		System.out.println("dateTraitMap: " + dateTraitsMap);
		// Create string in JSON format to emit
    	Gson gson = new Gson();
    	Type dateTraitValueType = new TypeToken<Map<String, Collection<String>>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
		String dateTraitString = gson.toJson(dateTraitsMap, dateTraitValueType);
		System.out.println("JSON string of dateTraitMap: " + dateTraitString);
		
		Map<String, String> varValueMap = new HashMap<String, String>();
		for(String s:variableList) {
			varValueMap.put(s, dateTraitString);
		}
				
    	Type varValueType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
		
    	String varValueString = gson.toJson(varValueMap, varValueType);
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
