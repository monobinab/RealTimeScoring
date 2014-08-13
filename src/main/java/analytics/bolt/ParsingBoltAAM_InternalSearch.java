package analytics.bolt;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ParsingBoltAAM_InternalSearch  extends BaseRichBolt{
	/**
	 * Created by Rock Wasserman 6/24/2014
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
    //private Map<String,Collection<String>> l_idToKeyWordMap; // USED TO MAP BETWEEN l_id AND THE DIVISION AND LINE ASSOCIATED WITH THAT ID UNTIL A NEW UUID IS FOUND
    private Map<String,Collection<String>> l_idToPidMap; //after a single search is mapped, the PIDs are put into this collection
    
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
//            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        	mongoClient = new MongoClient("trprrta2mong4.vm.itg.corp.us.shldcorp.com", 27000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        //db = mongoClient.getDB("RealTimeScoring");
	    //db.authenticate("rtsw", "5core123".toCharArray());
        db = mongoClient.getDB("test");
        memberCollection = db.getCollection("memberVariables");
        memberUUIDCollection = db.getCollection("memberUUID");
        
        this.currentUUID=null;
        this.current_l_id=null;

        //l_idToKeyWordMap = new HashMap<String,Collection<String>>();
        l_idToPidMap = new HashMap<String,Collection<String>>();
        
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

		// 1) SPLIT INPUT STRING
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD KEY WORDS TO LIST AND RETURN
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT KEY WORDS LIST AND EMIT VARIABLES
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
		// 5) POPULATE PID COLLECTION WITH THE FIRST KEY WORDS
		
	
		System.out.println("PARSING DOCUMENT -- INTERNAL SEARCH RECORD " + input.getString(0));
		
		// 1) SPLIT INPUT STRING
        String searchRec = input.getString(1);
        String searchSplitRec[] = splitRec(searchRec);

        //NOT USED FOR ANYTING OTHER THAN TO CHECK HOW THE RECORD WAS SPLIT
//        String splitRec = new String();
//        for(int i=0; i < searchSplitRec.length; i++) {
//        	if(i==0) splitRec = searchSplitRec[i];
//        	else splitRec = splitRec + " ~~~ " + searchSplitRec[i];
//        }
//        System.out.println("  split string: " + splitRec);
		
        
        //2014-03-08 10:56:17,00000388763646853831116694914086674166,743651,US,Sears
        if(searchSplitRec == null || searchSplitRec.length==0) {
        	return;
        }
        
        
		// 2) IF THE CURRENT RECORD HAS THE SAME UUID AS PREVIOUS RECORD(S) THEN ADD KEY WORDS TO LIST AND RETURN
        if(this.currentUUID !=null && this.currentUUID.equalsIgnoreCase(searchSplitRec[1])) {
        	if(this.current_l_id == null) {
        		//System.out.println(" @@@ NULL l_id -- return");
        		return;
        	}
        	
			String[] searchKeyWords = splitKeyWords(searchSplitRec[2]);
            System.out.println(" @@@ PROCESSING KEY WORDS: " + searchSplitRec[2]);
            processKeyWordList(searchKeyWords);
        	return;
        }
        
		// 3) IF THE CURRENT RECORD HAS A DIFFERENT UUID THEN PROCESS THE CURRENT KEY WORDS LIST AND EMIT VARIABLES
        if(l_idToPidMap != null && !l_idToPidMap.isEmpty()) {
        	Map<String,String> variableValueMap = processPidList();
        	if(variableValueMap!=null && !variableValueMap.isEmpty()) {
	        	Object variableValueJSON = createJsonFromVariableValueMap(variableValueMap);
	        	List<Object> listToEmit = new ArrayList<Object>();
	        	listToEmit.add(current_l_id);
	        	listToEmit.add(variableValueJSON);
	        	listToEmit.add("AAM_InternalSearch");
	        	System.out.println(" @@@ PARSING BOLT EMITTING: " + listToEmit);
	        	this.outputCollector.emit(listToEmit);

        	}
        	else {
        		System.out.println(" @@@ NO VARIBALES FOUND - NOTHING TO EMIT");
        	}
        	l_idToPidMap.remove(current_l_id);
            this.currentUUID=null;
            this.current_l_id=null;
        }
        
		// 4) IDENTIFY MEMBER BY UUID - IF NOT FOUND THEN SET CURRENT UUID FROM RECORD, SET CURRENT l_id TO NULL AND RETURN
        
        long t1 = System.currentTimeMillis();
        DBObject uuid = memberUUIDCollection.findOne(new BasicDBObject("u",searchSplitRec[1]));
        long t2 = System.currentTimeMillis() - t1;
        System.out.println(" @@@ Time to search for UUID: " + t2);
        
        if(uuid == null) {
            System.out.println(" @@@ COULD NOT FIND UUID");
            this.currentUUID=searchSplitRec[1];
        	this.current_l_id=null;
//        	this.current_l_id=currentUUID; // REMOVE ME!!!
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
			interactionDateTime = dateTimeFormat.parse(searchSplitRec[0]);
		} catch (ParseException e) {
			e.printStackTrace();
		}
        
        //System.out.println(" @@@ FOUND l_id: " + l_id + " interaction time: " + interactionDateTime + " key words: " + searchSplitRec[2]);
//        this.current_l_id = l_id;
        
        
		// 5) POPULATE l_id TO PID MAP, PROCESS KEY WORDS AND ADD FIRST PIDs TO COLLECTION
        l_idToPidMap.put(this.current_l_id, new ArrayList<String>());
        String[] firstKeyWords = splitKeyWords(searchSplitRec[2]);
        if(firstKeyWords != null && firstKeyWords.length>0) {
        	processKeyWordList(firstKeyWords);
        }
        else {
        	return;
        }
        
        //System.out.println(" @@@ PUT IN FIRST RECORD: " + this.current_l_id + " trait: " + firstTrait);
        
        return;
        
	}

    private String[] splitKeyWords(String keyWords) {
        String split[]=StringUtils.split(keyWords,"+");
        
        if(split !=null && split.length>0) {
			return split;
		}
		else {
			return null;
		}
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

    private boolean processKeyWordList(String[] search) {
    	
    	String queryResultsDoc = new String();
    	
    	//CONSTRUCT URL - queries Solr
		String URL1 = "http://solrx308p.stress.ch3.s.com:8180/search/select?qt=search&wt=json&q=";
		String URL2 = "&start=0&rows=50&fq=catalogs:%28%2212605%22%29&sort=instock%20desc,score%20desc,revenue%20desc&sortPrefix=L6;S4;10153&globalPrefix=L6,S4,10153&spuAvailability=S4&lmpAvailability=L6&fvCutoff=22&fqx=!%28storeAttributes:%28%2210153_DEFAULT_FULFILLMENT=SPU%22%29%20AND%20storeOrigin:%28%22Kmart%22%29%29&site=prod";
		String query = new String();
		
		query = URL1;
		int countKeyWords=0;
		for(String keyWord:search) {
			if(!keyWord.equalsIgnoreCase("N/A")) {
				countKeyWords++;
				if(countKeyWords==1) {
					query = query + keyWord;
				}
				else {
					query = query + "%20" + keyWord;
				}
			}
		}
		query = query + URL2;
		
		if(countKeyWords>0) {
		
			try {
				//System.out.println(query);
				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				long t1 = System.currentTimeMillis();
				Document doc = Jsoup.connect(query).get();
				long t2 = System.currentTimeMillis() - t1;
				System.out.println(" @@@ Query time: " + t2);
				doc.body().wrap("<pre></pre>");
				String text = doc.text();
				// Converting nbsp entities
				text = text.replaceAll("\u00A0", " ");
				
				queryResultsDoc = text;
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if(queryResultsDoc==null) {
				System.out.println("query results null");
			}
			
			else {
				if(isJSONValid(queryResultsDoc)) {
					if(new JsonParser().parse(queryResultsDoc).isJsonObject()) {
						JsonObject queryResultsToJson = new JsonParser().parse(queryResultsDoc).getAsJsonObject();
						if(queryResultsToJson.get("response")!=null && queryResultsToJson.get("response").isJsonObject()) {
							JsonObject response = queryResultsToJson.get("response").getAsJsonObject();
							if(response.get("docs").isJsonArray()) {
								JsonArray docs = response.getAsJsonArray("docs").getAsJsonArray();
								for(JsonElement doc:docs){
									if(doc.isJsonObject() && doc.getAsJsonObject().get("partnumber") != null) {
										if(!l_idToPidMap.get(current_l_id).contains(doc.getAsJsonObject().get("partnumber").toString().replace("\"", ""))) {
											l_idToPidMap.get(current_l_id).add(doc.getAsJsonObject().get("partnumber").toString().replace("\"", ""));
										}
									}
								}
								return true;
							}
						}
					}
				}
			}
		}
		return false;
    }

    private boolean isJSONValid(String test) {
    	 
        try {
            new JSONObject(test);
            return true;
        } catch (JSONException ex) {
            try {
                new JSONArray(test);
                return true;
            } catch (JSONException ex1) {
                //System.out.println(test);
                return false;
            }
        }
    }
        	
    private Map<String, String> processPidList() {
    	
    	Map<String,String> variableValueMap = new HashMap<String,String>();
//    	String div = new String(); //holds the substring of the first 3 characters from the PID
//    	Collection<String> var = new ArrayList<String>(); //variable collection
//    	
//    	for(String pid: l_idToPidMap.get(current_l_id)) {
//    		System.out.println(pid);
//    		if(pid.substring(pid.length() - 1).equals("P") && isNumbers(pid.substring(0,3))) {
//    			div = pid.substring(0,3);
//	    		if(divLnVariablesMap.containsKey(div)) {
//	    			var = divLnVariablesMap.get(div);
//	    			for(String v:var) {
//		    			if(variableValueMap.containsKey(var)) {
//		    				int value = 1 + Integer.valueOf(variableValueMap.get(v));
//		    				variableValueMap.remove(v);
//		    				variableValueMap.put(v, String.valueOf(value));
//		    			}
//		    			else {
//		    				variableValueMap.put(v, "1");
//		    			}
//	    			}
//	    		}
//    		}
//    	}
    	
    	
    	
    	for(String pid: l_idToPidMap.get(current_l_id)) {
    		//System.out.println(pid);
    		DBObject divLnDBO = pidDivLnCollection.findOne(new BasicDBObject().append("pid", pid));
    		if(divLnDBO != null) {
	    		String div = divLnDBO.get("d").toString();
	    		String divLn = divLnDBO.get("l").toString();
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
    
    
	private boolean isNumbers(String s) {
		for(int i=0;i<s.length();i++) {
			if(!s.substring(i, i+1).equals("0")
					&& !s.substring(i, i+1).equals("1")
					&& !s.substring(i, i+1).equals("2")
					&& !s.substring(i, i+1).equals("3")
					&& !s.substring(i, i+1).equals("4")
					&& !s.substring(i, i+1).equals("5")
					&& !s.substring(i, i+1).equals("6")
					&& !s.substring(i, i+1).equals("7")
					&& !s.substring(i, i+1).equals("8")
					&& !s.substring(i, i+1).equals("9")
			) return false;
		}
		return true;
	}

	private Object createJsonFromVariableValueMap(Map<String, String> variableValueMap) {
		// Create string in JSON format to emit
    	Gson gson = new Gson();
    	Type varValueType = new TypeToken<Map<String, String>>() {private static final long serialVersionUID = 1L;}.getType();
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
