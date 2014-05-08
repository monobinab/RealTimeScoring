package analytics.bolt;

import analytics.util.Change;
import analytics.util.RealTimeScoringContext;
import analytics.util.TransactionLineItem;
import analytics.util.Variable;
import analytics.util.strategies.Strategy;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.*;

import redis.clients.jedis.Jedis;
import shc.npos.segments.Segment;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import java.security.SignatureException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;


public class StrategyBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    MongoClient mongoClient;
    DBCollection modelCollection;
    DBCollection memberVariablesCollection;
    DBCollection variablesCollection;
    DBCollection divLnVariableCollection;
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

        System.out.println("PREPARING STRATEGY BOLT");
        
        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
        //db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
	    db.authenticate("rtsw", "5core123".toCharArray());
        modelCollection = db.getCollection("modelVariables");
        memberVariablesCollection = db.getCollection("memberVariables");
        variablesCollection = db.getCollection("Variables");
        divLnVariableCollection = db.getCollection("divLnVariable");
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
		
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		// 2) FETCH MEMBER VARIABLES FROM memberVariables COLLECTION
		// 3) CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		// 4) FETCH CHANGED VARIABLES FROM changedMemberVariables COLLECTION
		// 5) CREATE MAP FROM CHANGED VARIABLES TO VALUE AND EXPIRATION DATE (CHANGE CLASS)
		// 6) CREATE MAP FROM NEW CHANGES TO CHANGE CLASS
		// 7) FOR EACH CHANGE EXECUTE STRATEGY
		// 8) FORMAT DOCUMENT FOR MONGODB UPSERT
		// 9) FIND ALL MODELS THAT ARE AFFECTED BY CHANGES
		// 10) EMIT LIST OF MODEL IDs
		
		
		List<TransactionLineItem> lineItemList = restoreLineItemListFromJson(input.getString(0));
		
		//List<TransactionLineItem> lineItemList = (List<TransactionLineItem>) input.getValueByField("lineItemList");
		//List<TransactionLineItem> lineItemList = new ArrayList<TransactionLineItem>();

		System.out.println("APPLYING STRATEGIES");
		System.out.println(" *** input tuple: " + input);
		System.out.println(" *** line items: " + lineItemList.size());
		for(TransactionLineItem i: lineItemList) {
			System.out.println(" variables: " + i.getVariableList().toString());
		}
		
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD IN lineItemList
		String l_id = lineItemList.get(0).getL_id();
		
		// 2) FETCH MEMBER VARIABLES FROM memberVariables COLLECTION
		DBObject mbrVariables = memberVariablesCollection.findOne(new BasicDBObject("l_id",l_id));
		if(mbrVariables == null) {
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
		if(changedMbrVariables!=null && changedMbrVariables.keySet()!=null) {
			Iterator<String> collectionChangesIter = changedMbrVariables.keySet().iterator();
		    
			while (collectionChangesIter.hasNext()){
		    	String key = collectionChangesIter.next();
		    	//skip expired changes
		    	if("_id".equals(key) || "l_id".equals(key)) {
		    		continue;
		    	}
		    	try {
					if(!simpleDateFormat.parse(((DBObject) changedMbrVariables.get(key)).get("e").toString()).after(new Date())) {
						allChanges.put(key.toUpperCase(), new Change(key.toUpperCase(), ((DBObject) changedMbrVariables.get(key)).get("v"), simpleDateFormat.parse(((DBObject) changedMbrVariables.get(key)).get("e").toString())));
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		}
        	
        
		// 6) CREATE MAP FROM NEW CHANGES TO CHANGE CLASS
        Map<String,Change> newChanges = new HashMap<String,Change>();
		for(TransactionLineItem lineItem: lineItemList) {
	        RealTimeScoringContext context = new RealTimeScoringContext();
	        context.setTransactionLineItem(lineItem);
	        context.setPreviousValue(0);
	        for(String variableName: lineItem.getVariableList()) {
                DBObject variableFromVariablesCollection = variablesCollection.findOne(new BasicDBObject("name", variableName));
                if (variableFromVariablesCollection != null )System.out.println(" found variable :" + variableName.toUpperCase());

        		// 7) FOR EACH CHANGE EXECUTE STRATEGY
                try {
                    //arbitrate between memberVariables and changedMemberVariables to send as previous value
                	if(variableModelsMap.containsKey(variableName)) {
                		Strategy strategy = (Strategy) Class.forName("analytics.util.strategies."+ variableFromVariablesCollection.get("strategy")).newInstance();
                        if(allChanges.containsKey(variableName)) {
                        	context.setPreviousValue(allChanges.get(variableName.toUpperCase()).getValue());
                        }
                        else {
                        	context.setPreviousValue(memberVariablesMap.get(variableName.toUpperCase()));
                        }
                        
                        newChanges.put(variableName, strategy.execute(context));
                	}
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
	        }
		}
	            	
		// 8) FORMAT DOCUMENT FOR MONGODB UPSERT
        if(!newChanges.isEmpty()){
            System.out.println(" CHANGES: " + newChanges );
            
			Iterator<Entry<String, Change>> newChangesIter = newChanges.entrySet().iterator();
			BasicDBObject newDocument = new BasicDBObject();
		    while (newChangesIter.hasNext()) {
		        Map.Entry<String, Change> pairsVarValue = (Map.Entry<String, Change>)newChangesIter.next();
		    	String varNm = pairsVarValue.getKey().toString().toUpperCase();
				Object val = pairsVarValue.getValue().value;
				newDocument.append(varNm, new BasicDBObject().append("v", val).append("e", pairsVarValue.getValue().getExpirationDateAsString()));
		    	
		    	allChanges.put(varNm, new Change(varNm, val, pairsVarValue.getValue().expirationDate));
		    }

		    BasicDBObject searchQuery = new BasicDBObject().append("l_id", l_id);
		    
		    System.out.println("DOCUMENT TO INSERT:");
		    System.out.println(newDocument.toString());
		    System.out.println("END DOCUMENT");
		    
		    //upsert document
		    changedVariablesCollection.update(searchQuery, new BasicDBObject("$set", newDocument), true, false);


			// 9) FIND ALL MODELS THAT ARE AFFECTED BY CHANGES
            List<Object> modelIdList = new ArrayList<Object>();
            modelIdList.add(l_id);
            for(String changedVariable:newChanges.keySet())
            {
                //TODO: do not put variables that are not associated with a model in the changes map
            	Collection<Integer> models = variableModelsMap.get(changedVariable);
                for (Integer modelId: models){
                    if(!modelIdList.contains(modelId)) {
	                	modelIdList.add(modelId);
                    }
                }
            }
    		
            // 10) EMIT LIST OF MODEL IDs
            if(modelIdList.size()>1) {
            	List<Object> stringModelList = new ArrayList<Object>();
            	stringModelList.add(createStringFromModelList(modelIdList));
            	System.out.println(" *** strategy bolt emitting: " + stringModelList);
            	this.outputCollector.emit(stringModelList);
            }
        }
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
		declarer.declare(new Fields("modelIdList"));
		
	}
    
	public static List<TransactionLineItem> restoreLineItemListFromJson(String json)
    {
        //System.out.println(" JSON string: " + json);
		List<TransactionLineItem> lineItemList = new ArrayList<TransactionLineItem>();
        Type lineItemListType = new TypeToken<List<TransactionLineItem>>() {}.getType();

        lineItemList = new Gson().fromJson(json, lineItemListType);
        return lineItemList;
    }
	
    private Object createStringFromModelList(List<Object> modelList) {
		// Create string in JSON format to emit
   	
    	String transLineItemListString=StringUtils.join(modelList.toArray(),",");
    	
    	/*
    	Gson gson = new Gson();
    	Type transLineItemType = new TypeToken<List<Object>>() {}.getType();
    	String transLineItemListString = gson.toJson(modelList, transLineItemType);
    	*/
		return transLineItemListString;
	}


}
