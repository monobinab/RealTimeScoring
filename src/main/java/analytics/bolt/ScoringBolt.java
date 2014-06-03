/**
 * 
 */
package analytics.bolt;

import analytics.util.Change;
import analytics.util.Variable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ScoringBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    MongoClient mongoClient;
    DBCollection modelCollection;
    DBCollection memberVariablesCollection;
    DBCollection memberScoreCollection;
    DBCollection variablesCollection;
    DBCollection changedVariablesCollection;
    DBCollection changedMemberScoresCollection;
    
    
    private Map<String,Collection<Integer>> variableModelsMap;
    private Map<String, String> variableVidToNameMap;

    private Jedis jedis;




    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public void setModelCollection(DBCollection modelCollection) {
        this.modelCollection = modelCollection;
    }

    public void setMemberCollection(DBCollection memberCollection) {
        this.memberVariablesCollection = memberCollection;
    }

    public void setMemberScoreCollection(DBCollection memberScoreCollection) {
        this.memberScoreCollection = memberScoreCollection;
    }

    public void setVariablesCollection(DBCollection variablesCollection) {
        this.variablesCollection = variablesCollection;
    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
//        this.outputCollector.emit(tuple);
	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
        
        //System.out.println("PREPARING SCORING BOLT");

        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
        //db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
	    db.authenticate("rtsw", "5core123".toCharArray());
        memberVariablesCollection = db.getCollection("memberVariables");
        modelCollection = db.getCollection("modelVariables");
        memberScoreCollection = db.getCollection("memberScore");
        variablesCollection = db.getCollection("Variables");
        changedVariablesCollection = db.getCollection("changedMemberVariables");
        changedMemberScoresCollection = db.getCollection("changedMemberScores");

        // populate the variableModelsMap
        variableModelsMap = new HashMap<String, Collection<Integer>>();
        DBCursor models = modelCollection.find();
        for(DBObject model:models){
             BasicDBList modelVariables = (BasicDBList) model.get("variable");
             for(Object modelVariable:modelVariables)
             {
                 String variableName = ((DBObject) modelVariable).get("name").toString().toUpperCase();
                 if (variableModelsMap.get(variableName) == null)
                 {
                     Collection<Integer> modelIds = new ArrayList<Integer>();
                     addModel(model, variableName.toUpperCase(), modelIds);
                 }
                 else
                 {
                     Collection<Integer> modelIds = variableModelsMap.get(variableName.toUpperCase());
                     addModel(model, variableName.toUpperCase(), modelIds);
                 }
             }
        }

        //System.out.println(" variablesModelMap: " + variableModelsMap);

        // populate the variableVidToNameMap
        variableVidToNameMap = new HashMap<String, String>();
        DBCursor vCursor = variablesCollection.find();
        for(DBObject variable:vCursor){
             String variableName = ((DBObject) variable).get("name").toString().toUpperCase();
             String vid = ((DBObject) variable).get("VID").toString();
             if (variableName != null && vid != null)
             {
                 variableVidToNameMap.put(vid, variableName.toUpperCase());
             }
        }

        //System.out.println(" variableVidToNameMap: " + variableVidToNameMap);


        //jedis = new Jedis("151.149.116.48");

    }

    private void addModel(DBObject model, String variableName, Collection<Integer> modelIds) {
        modelIds.add(Integer.valueOf(model.get("modelId").toString()));
        variableModelsMap.put(variableName.toUpperCase(), modelIds);
    }

    /*
     * (non-Javadoc)
     *
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
	@Override
	public void execute(Tuple input) {
		
		String l_id = input.getString(0);
		String source = input.getString(2);
		
		
		// SCORING BOLTS READS A LIST OF OBJECTS WITH THE FIRST ELEMENT BEING THE HASHED LOYALTY ID
		// AND n MODEL IDs AFTER
		List<String> modelIdList = restoreModelListFromJson(input.getString(1));
		//List<TransactionLineItem> lineItemList = new ArrayList<TransactionLineItem>();

		//System.out.println("RE-SCORING MODELS");
		//System.out.println(" *** model ID list: " + modelIdList);
		
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		
		// 2) FETCH MEMBER VARIABLES FROM memberVariables COLLECTION
		DBObject mbrVariables = memberVariablesCollection.findOne(new BasicDBObject("l_id",l_id));
		if(mbrVariables == null) {
			//System.out.println(" ### SCORING BOLT COULD NOT FIND MEMBER VARIABLES");
			return;
		}
		
		// 3) CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String,Object> memberVariablesMap = new HashMap<String,Object>();
		Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
		while(mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if(!key.equals("l_id") && !key.equals("_id")) {
				memberVariablesMap.put(variableVidToNameMap.get(key).toUpperCase(), mbrVariables.get(key));
			}
		}
		
		// 4) FETCH CHANGED VARIABLES FROM changedMemberVariables COLLECTION
		DBObject changedMbrVariables = changedVariablesCollection.findOne(new BasicDBObject("l_id",l_id));

		// 5) CREATE MAP FROM CHANGED VARIABLES TO VALUE AND EXPIRATION DATE (CHANGE CLASS)
		Map<String,Change> allChanges = new HashMap<String,Change>();
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
    	//System.out.println(" ### CHANGED MEMBER VARIABLES: " + changedMbrVariables);
    	if(changedMbrVariables!=null && changedMbrVariables.keySet()!=null) {
			Iterator<String> collectionChangesIter = changedMbrVariables.keySet().iterator();
		    
			while (collectionChangesIter.hasNext()){
		    	String key = collectionChangesIter.next();
		    	//skip expired changes
		    	if("_id".equals(key) || "l_id".equals(key)) {
		    		continue;
		    	}
//		    	System.out.println("   ### VARIABLE: " + key);
//		    	System.out.println("   ### GET VARIABLE: " + changedMbrVariables.get(key));
//		    	System.out.println("   ### EXPIRATION: " + ((DBObject) changedMbrVariables.get(key)).get("e"));
		    	try {
//			    	System.out.println("   ### THE WHOLE THING: " + simpleDateFormat.parse(((DBObject) changedMbrVariables.get(key)).get("e").toString()));
					if(simpleDateFormat.parse(((DBObject) changedMbrVariables.get(key)).get("e").toString()).after(Calendar.getInstance().getTime())) {
						allChanges.put(key.toUpperCase()
								, new Change(key.toUpperCase()
								, ((DBObject) changedMbrVariables.get(key)).get("v")
								, simpleDateFormat.parse(((DBObject) changedMbrVariables.get(key)).get("e").toString())));
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		}
//		System.out.println(" *** ALL CHANGES MAP: " + allChanges);
	
        // Score each model in a loop
		BasicDBObject updateRec = new BasicDBObject();
        for (String modelId:modelIdList)
        {
        	// recalculate score for model
        	
        	//System.out.println(" ### SCORING MODEL ID: " + modelId);
            double newScore = 1/(1+ Math.exp(-1*(calcMbrVar(memberVariablesMap, allChanges,  Integer.valueOf(modelId))))) * 1000;
            //System.out.println(l_id + ": " + Double.toString(newScore));
            
            // FIND THE MIN AND MAX EXPIRATION DATE OF ALL VARIABLE CHANGES FOR CHANGED MODEL SCORE TO WRITE TO SCORE CHANGES COLLECTION
			Date minDate = null;
			Date maxDate = null;
            for(String key: allChanges.keySet()) {
            	if(variableModelsMap.get(key).contains(Integer.valueOf(modelId))) {
            		if(minDate == null) {
            			minDate = allChanges.get(key).getExpirationDate();
            			maxDate = allChanges.get(key).getExpirationDate();
            		}
            		else {
            			if(allChanges.get(key).getExpirationDate().before(minDate)) {
                			minDate = allChanges.get(key).getExpirationDate();
                		}
            			if(allChanges.get(key).getExpirationDate().after(maxDate)) {
                			maxDate = allChanges.get(key).getExpirationDate();
                		}
            		}
            	}
            }
	                            
            //APPEND CHANGED SCORE AND MIN/MAX EXPIRATION DATES TO DOCUMENT FOR UPDATE
            updateRec.append(modelId.toString(), new BasicDBObject().append("s", newScore).append("minEx", simpleDateFormat.format(minDate)).append("maxEx", simpleDateFormat.format(maxDate)));
            
            DBObject oldScore = changedMemberScoresCollection.findOne(new BasicDBObject("l_id", l_id));
            String message = new StringBuffer().append(l_id).append("-").append(modelId).append("-").append(oldScore == null ? "0" : oldScore.get("1")).append("-").append(newScore).toString();
            
            // EMIT CHANGES
        	List<Object> listToEmit = new ArrayList<Object>();
        	listToEmit.add(l_id);
        	listToEmit.add(oldScore == null ? "0" : oldScore.get("1"));
        	listToEmit.add(newScore);
        	listToEmit.add(modelId);
        	listToEmit.add(source);
        	//System.out.println(" ### scoring bolt emitting: " + listToEmit);
        	this.outputCollector.emit(listToEmit);

            
            //System.out.println(message);
            //jedis.publish("score_changes", message);
        }
    	//System.out.println(" ### UPDATE RECORD CHANGED SCORE: " + updateRec);
        if(updateRec != null) {
        	changedMemberScoresCollection.update(new BasicDBObject("l_id", l_id), new BasicDBObject("$set", updateRec), true, false);

        }
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


    /*
      * (non-Javadoc)
      *
      * @see
      * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
      * topology.OutputFieldsDeclarer)
      */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","oldScore","newScore","model","source"));
		
	}

	public static List<String> restoreModelListFromJson(String json)
    {
        //System.out.println(" ### MODEL LIST STRING: " + json);
		//modelList = new ArrayList<Object>();
        
        String strings[]=StringUtils.split(json,",");
        List<String> modelList = new ArrayList<String>();
        for(String s: strings) {
        	modelList.add(s);
        }
        //System.out.println(" ### MODEL LIST PARSED: " + json);
        /*
        Type lineItemListType = new TypeToken<List<Object>>() {}.getType();
        List<Object> modelList = new Gson().fromJson(json, lineItemListType);
        */
        return modelList;
    }
	

    double calcMbrVar( Map<String,Object> mbrVarMap, Map<String,Change> changes, int modelId)
    {
	    
        BasicDBObject queryModel = new BasicDBObject("modelId", modelId);
	    DBCursor modelCollectionCursor = modelCollection.find( queryModel );

        DBObject model = null;
	    while( modelCollectionCursor.hasNext() ) {
	    	model = ( BasicDBObject ) modelCollectionCursor.next();
	    }
	    
	    //System.out.println( model.get( "modelId" ) + ": " + model.get( "constant" ).toString());
	    
	    double val = (Double) model.get( "constant" );
	    
	    BasicDBList variable = ( BasicDBList ) model.get( "variable" );
	    Variable var = new Variable();
	    
	    for( Iterator< Object > it = variable.iterator(); it.hasNext(); )
	    {
	    	BasicDBObject dbo     = ( BasicDBObject ) it.next();
	    	BasicDBObject queryVariableId = new BasicDBObject("name", dbo.get("name").toString().toUpperCase());
		    
	    	DBObject variableFromVariablesCollection = this.variablesCollection.findOne(queryVariableId);
            var.setVid(variableFromVariablesCollection.get("VID").toString());
            var.makePojoFromBson( dbo );

//		    System.out.println( var.getName() + ", " + var.getRealTimeFlag() + ", " + var.getType()  + ", " + var.getStrategy() + ", " + var.getCoefficeint() +", " + var.getVid());
//		    System.out.println("PASS: " + mbrVarMap + " varNm: " + var.getName() + " varType: " + var.getType() + " cng: " + changes + var.getCoefficeint());
		    if(  var.getType().equals("Integer")) val = val + ((Integer)calculateVariableValue(mbrVarMap, var, changes, var.getType()) * var.getCoefficeint());
		    else if( var.getType().equals("Double")) val = val + ((Double)calculateVariableValue(mbrVarMap, var, changes, var.getType()) * var.getCoefficeint());
		    else {
		    	val = 0;
		    	break;
		    }
	    }
        return val;
    }


	private Object calculateVariableValue(Map<String,Object> mbrVarMap, Variable var, Map<String,Change> changes, String dataType) {
		Object changedValue = null;
		if(var != null) {
			if(changes.containsKey(var.getName().toUpperCase())) {
				changedValue = changes.get(var.getName().toUpperCase()).getValue();
				//System.out.println("changed variable: " + var.getName().toUpperCase() + "  value: " + changedValue);
			}
			if(changedValue == null) {
				changedValue=mbrVarMap.get(var.getName().toUpperCase());
			}
			else{
				if(dataType.equals("Integer")) {
					//changedValue=Integer.parseInt(changedValue.toString());
					changedValue=(int) Math.round(Double.valueOf(changedValue.toString()));
				}
				else {
					changedValue=Double.parseDouble(changedValue.toString());
				}
			}
		}
		else {
			return 0;
		}
		return changedValue;
	}
	
}
