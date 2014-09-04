package analytics.bolt;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import analytics.util.MongoUtils;
import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;
import analytics.util.strategies.Strategy;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class StrategyBolt extends BaseRichBolt {

	static final Logger logger = Logger
			.getLogger(StrategyBolt.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    private DBCollection modelVariablesCollection;
    private DBCollection memberVariablesCollection;
    private DBCollection variablesCollection;
    private DBCollection divLnVariableCollection;
    private DBCollection changedVariablesCollection;
    private DBCollection changedMemberScoresCollection;
    private DBCollection divCatVariableCollection;
    private Map<String,Collection<Integer>> variableModelsMap;
    private Map<String,Collection<Integer>> modelVariablesMap;
    private Map<String, String> variableVidToNameMap;
    private Map<String, String> variableNameToVidMap;

    private Jedis jedis;

    
    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void setModelCollection(DBCollection modVariablesCollection) {
        this.modelVariablesCollection = modVariablesCollection;
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
			db = MongoUtils.getClient("DEV");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

        modelVariablesCollection = db.getCollection("modelVariables");
        memberVariablesCollection = db.getCollection("memberVariables");
        variablesCollection = db.getCollection("Variables");
        divLnVariableCollection = db.getCollection("divLnVariable");
        divCatVariableCollection = db.getCollection("divCatVariable");
        changedVariablesCollection = db.getCollection("changedMemberVariables");
        changedMemberScoresCollection = db.getCollection("changedMemberScores");
        
        
        // populate the variableModelsMap
        variableModelsMap = new HashMap<String, Collection<Integer>>();
        modelVariablesMap = new HashMap<String, Collection<Integer>>();
        DBCursor models = modelVariablesCollection.find();
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
        variableNameToVidMap = new HashMap<String, String>();
        DBCursor vCursor = variablesCollection.find();
        for(DBObject variable:vCursor){
			String variableName = ((DBObject) variable).get("name").toString().toUpperCase();
			String vid = ((DBObject) variable).get("VID").toString();
			if (variableName != null && vid != null)
			{
				variableVidToNameMap.put(vid, variableName.toUpperCase());
				variableNameToVidMap.put(variableName.toUpperCase(),vid);
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
		System.out.println("STRATEGY BOLT GOT " + input.toString());
		logger.info("The time it enters inside Strategy Bolt execute method"+System.currentTimeMillis());
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
		
		//logger.info("APPLYING STRATEGIES");
		
		// 1) PULL OUT HASHED LOYALTY ID FROM THE FIRST RECORD
		String l_id = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String messageID = "";
		if(input.contains("messageID")){
			messageID = input.getStringByField("messageID");
		}
		Map<String, String> newChangesVarValueMap = restoreVariableListFromJson(input.getString(1));

		//logger.info(" ~~~ STRATEGY BOLT PARSED VARIABLE MAP AS: " + varAmountMap);
		//logger.info(" ~~~ input tuple: " + input);
		//logger.info(" ~~~ line items: " + lineItemList.size());
		
		
		// 2) FETCH MEMBER VARIABLES FROM memberVariables COLLECTION
		BasicDBObject variableFields = new BasicDBObject("l_id", 1);
		variableFields.append("_id", 0);
		for(String v:newChangesVarValueMap.keySet()) {
			variableFields.append(variableNameToVidMap.get(v), 1);
		}
		
		//logger.info("Member Variables for Loyality ID is..."+l_id+"variableFields  are ...."+variableFields.toString());
		DBObject mbrVariables = memberVariablesCollection.findOne(new BasicDBObject("l_id",l_id),variableFields);
		if(mbrVariables == null) {
			logger.info(" ~~~ STRATEGY BOLT COULD NOT FIND MEMBER VARIABLES");
			return;
		}
		
		// 3) CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String,Object> memberVariablesMap = new HashMap<String,Object>();
		Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
		while(mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if(!key.equals("l_id") && !key.equals("_id")) {
				if(mbrVariables.get(key) != null) {
					memberVariablesMap.put(variableVidToNameMap.get(key).toUpperCase(), mbrVariables.get(key));
				}
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
						allChanges.put(key.toUpperCase()
								, new Change(key.toUpperCase()
									, ((DBObject) changedMbrVariables.get(key)).get("v")
									, simpleDateFormat.parse(((DBObject) changedMbrVariables.get(key)).get("e").toString())
									, simpleDateFormat.parse(((DBObject) changedMbrVariables.get(key)).get("f").toString())));
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
		    }
		}
        	
//		for(String v: varAmoutMapKeySet) {
//			System.out.println(" variable: " + v + "  amount: " + varAmountMap.get(v));
//		}
		        
		// 6) CREATE MAP FROM NEW CHANGES TO CHANGE CLASS
        Map<String,Change> newChanges = new HashMap<String,Change>();
        for(String newChangeVariableName: newChangesVarValueMap.keySet()) {
            DBObject variableFromVariablesCollection = variablesCollection.findOne(new BasicDBObject("name", newChangeVariableName.toUpperCase()));
            if (variableFromVariablesCollection == null ) {
            	logger.info(" ~~~ DID NOT FIND VARIBALE: ");
            	continue;
            }
        	//logger.info(" ~~~ FOUND VARIABLE - name: " + variableName + " varValueMap: "  + varValueMap.get(variableName));
            
	        RealTimeScoringContext context = new RealTimeScoringContext();
            context.setValue(newChangesVarValueMap.get(newChangeVariableName));
	        context.setPreviousValue(0);

    		// 7) FOR EACH CHANGE EXECUTE STRATEGY
            try {
                //arbitrate between memberVariables and changedMemberVariables to send as previous value
            	if(variableModelsMap.containsKey(newChangeVariableName)) {
            		if(variableFromVariablesCollection.get("strategy").equals("NONE")) {
            			continue;
            		}
            		
            		Strategy strategy = (Strategy) Class.forName("analytics.util.strategies."+ variableFromVariablesCollection.get("strategy")).newInstance();
                    if(allChanges.containsKey(newChangeVariableName)) {
                    	context.setPreviousValue(allChanges.get(newChangeVariableName.toUpperCase()).getValue());
                    }
                    else {
                    	if(memberVariablesMap.get(newChangeVariableName.toUpperCase()) != null) {
                    		context.setPreviousValue(memberVariablesMap.get(newChangeVariableName.toUpperCase()));
                    	}
                    }
                    logger.info(" ~~~ STRATEGY BOLT CHANGES - context: " + context);
                    newChanges.put(newChangeVariableName, strategy.execute(context));
            	}
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
	            	
		// 8) FORMAT DOCUMENT FOR MONGODB UPSERT
        if(!newChanges.isEmpty()){
//            System.out.println(" ~~~ CHANGES: " + newChanges );
            
			Iterator<Entry<String, Change>> newChangesIter = newChanges.entrySet().iterator();
			BasicDBObject newDocument = new BasicDBObject();
		    while (newChangesIter.hasNext()) {
		        Map.Entry<String, Change> pairsVarValue = (Map.Entry<String, Change>)newChangesIter.next();
		    	String varVid =  variableNameToVidMap.get(pairsVarValue.getKey().toString().toUpperCase());
				Object val = pairsVarValue.getValue().value;
				newDocument.append(varVid, new BasicDBObject().append("v", val).append("e", pairsVarValue.getValue().getExpirationDateAsString()).append("f", pairsVarValue.getValue().getEffectiveDateAsString()));
		    	
		    	allChanges.put(varVid, new Change(varVid, val, pairsVarValue.getValue().expirationDate));
		    }

		    BasicDBObject searchQuery = new BasicDBObject().append("l_id", l_id);
		    
		    logger.info(" ~~~ DOCUMENT TO INSERT:");
		    logger.info(newDocument.toString());
		    logger.info(" ~~~ END DOCUMENT");
		    
		    //upsert document
		    changedVariablesCollection.update(searchQuery, new BasicDBObject("$set", newDocument), true, false);


			// 9) FIND ALL MODELS THAT ARE AFFECTED BY CHANGES
            List<Object> modelIdList = new ArrayList<Object>();
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
            if(modelIdList.size()>0) {
            	List<Object> listToEmit = new ArrayList<Object>();
            	listToEmit.add(l_id);
            	listToEmit.add(createStringFromModelList(modelIdList));
            	listToEmit.add(source);
            	listToEmit.add(messageID);
            	logger.info(" ~~~ STRATEGY BOLT EMITTING: " + listToEmit);
            	this.outputCollector.emit(listToEmit);
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
		declarer.declare(new Fields("l_id","modelIdList","source","messageID"));
	
	}
    
	public static Map<String, String> restoreVariableListFromJson(String json)
    {
		Map<String, String> varList = new HashMap<String, String>();
        Type varListType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;}.getType();

        varList = new Gson().fromJson(json, varListType);
        logger.info(" JSON string: " + json);
        logger.info(" Map: " + varList);
        return varList;
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
