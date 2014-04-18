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
import com.ibm.jms.JMSMessage;
import com.mongodb.*;

import redis.clients.jedis.Jedis;
import shc.npos.segments.Segment;
import shc.npos.util.SegmentUtils;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.joda.time.Days;
import org.joda.time.LocalDate;

import java.math.BigInteger;
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
    DBCollection changesCollection;
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
        divLnVariableCollection = db.getCollection("DivLnVariable");
        changesCollection = db.getCollection("changedMemberVariables");
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

		List<TransactionLineItem> lineItemList = (List<TransactionLineItem>) input.getValueByField("lineItemList");
		//List<TransactionLineItem> lineItemList = new ArrayList<TransactionLineItem>();

		System.out.println("APPLYING STRATEGIES");
		
		// TODO: redo flow chart and put in comments
		
		String hashed = lineItemList.get(0).getHashed();
		
		DBObject mbrVariables = memberVariablesCollection.findOne(new BasicDBObject("l_id",hashed));
		if(mbrVariables == null) {
			return;
		}
		
		Map<String,Object> memberVariablesMap = new HashMap<String,Object>();
		Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
		while(mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if(!key.equals("l_id") && !key.equals("_id")) {
				memberVariablesMap.put(variableVidToNameMap.get(key).toUpperCase(), mbrVariables.get(key));
			}
		}
		
		//get changed variable values from the changedMemberVariables collection
		DBObject collectionChanges = changesCollection.findOne(new BasicDBObject("l_id",hashed));

		Map<String,Change> allChanges = new HashMap<String,Change>();
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		if(collectionChanges!=null && collectionChanges.keySet()!=null) {
			Iterator<String> collectionChangesIter = collectionChanges.keySet().iterator();
		    
			while (collectionChangesIter.hasNext()){
		    	String key = collectionChangesIter.next();
		    	//skip expired changes
		    	if("_id".equals(key) || "l_id".equals(key)) {
		    		continue;
		    	}
		    	try {
					if(!simpleDateFormat.parse(((DBObject) collectionChanges.get(key)).get("e").toString()).after(new Date())) {
						allChanges.put(key.toUpperCase(), new Change(key.toUpperCase(), ((DBObject) collectionChanges.get(key)).get("v"), simpleDateFormat.parse(((DBObject) collectionChanges.get(key)).get("e").toString())));
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		}
        	
        
        Map<String,Change> newChanges = new HashMap<String,Change>();
		
		//List<String> foundVariablesList = new ArrayList<String>();
		for(TransactionLineItem lineItem: lineItemList) {
	        RealTimeScoringContext context = new RealTimeScoringContext();
	        context.setTransactionLineItem(lineItem);
	        context.setPreviousValue(0);
	        for(String variableName: lineItem.getVariableList()) {
                DBObject variableFromVariablesCollection = variablesCollection.findOne(new BasicDBObject("name", variableName));
                if (variableFromVariablesCollection != null )System.out.println(" found variable :" + variableName.toUpperCase());

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


		    BasicDBObject searchQuery = new BasicDBObject().append("l_id", hashed);
		    
		    System.out.println("DOCUMENT TO INSERT:");
		    System.out.println(newDocument.toString());
		    System.out.println("END DOCUMENT");
		    changesCollection.update(searchQuery, new BasicDBObject("$set", newDocument), true, false);


            // find all the models that are affected by these changes
            Set<Integer> modelsSet = new HashSet<Integer>();
            for(String changedVariable:newChanges.keySet())
            {
                //TODO: do not put variables that are not associated with a model in the changes map
            	Collection<Integer> models = variableModelsMap.get(changedVariable);
                for (Integer modelId: models){
                    modelsSet.add(modelId);
                }
            }
            // TODO: emit model IDs collection
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


    private RealTimeScoringContext createRealTimeScoringContext(Segment segment) {
        String sellingAmountString = segment.getSegmentBody().get("Selling Amount").trim();
        Double sellingAmount = 0d;
        if (!sellingAmountString.contains("-"))
        {
           sellingAmount = Double.valueOf(sellingAmountString)/100;
        }

        TransactionLineItem transactionLineItem = new TransactionLineItem();
        transactionLineItem.setAmount(sellingAmount);
        RealTimeScoringContext context = new RealTimeScoringContext();
        context.setTransactionLineItem(transactionLineItem);
        context.setPreviousValue(0);
        return context;
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
		declarer.declare(new Fields("lineItems"));
		
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
				System.out.println("changed variable: " + var.getName().toUpperCase() + "  value: " + changedValue);
			}
			if(changedValue == null) {
				changedValue=mbrVarMap.get(var.getName().toUpperCase());
			}
			else{
				if(dataType.equals("Integer")) {
					changedValue=Integer.parseInt(changedValue.toString());
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
