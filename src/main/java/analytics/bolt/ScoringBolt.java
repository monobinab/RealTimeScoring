/**
 * 
 */
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

public class ScoringBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    MongoClient mongoClient;
    DBCollection modelCollection;
    DBCollection memberCollection;
    DBCollection memberScoreCollection;
    DBCollection variablesCollection;
    DBCollection divLnItmCollection;
    DBCollection divLnVariableCollection;
    DBCollection changesCollection;
    DBCollection changedMemberScoresCollection;
    
    private static final String encryptionMethod = "SHA-1";
    private static final String salt = new String (Base64.encodeBase64("timesnewroman".getBytes()));
    
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
        this.memberCollection = memberCollection;
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
        memberCollection = db.getCollection("memberVariables");
        modelCollection = db.getCollection("modelVariables");
        memberScoreCollection = db.getCollection("memberScore");
        variablesCollection = db.getCollection("Variables");
        divLnItmCollection = db.getCollection("DivLnItm");
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

        System.out.println(" variablesModelMap: " + variableModelsMap);

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

        System.out.println(" variableVidToNameMap: " + variableVidToNameMap);


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

		String l_id = null;
		JMSMessage document = (JMSMessage) input.getValueByField("npos");
			
			// 0 Test if transaction is a sale - if not return
			// 1 find SYWR ID for transaction OK
			// 2 if SYWR ID retrieve all items in the basket OK
			// 3 for each item in the basket find the division OK
			// 4 if any divisions that affects the HA model - then re-score OK
			// 5 store changes in mongodb 	OK
			// 6 store new score in mongodb  OK
			// 7 store the score in redis    PENDING

	        try {
	        	
	        	// 0 Test if transaction is a sale - if not return
	        	
	            String nposTransaction = ((TextMessage) document).getText();
	            //System.out.println(nposTransaction);
	            Collection<Segment> saleSegments = SegmentUtils.findAllSegments(nposTransaction, "B1");
	            boolean isSale = false;
	            for (Segment segment : saleSegments) {
	                String transactionType = segment.getSegmentBody().get("Transaction Type Code");
	                if ("1".equals(transactionType)) {
	                	isSale=true;
	                }
	            }
				
	            if(!isSale) {
	            	return;
	            }
	            
	            // 1 find SYWR ID for transaction OK

	            Collection<Segment> b2Segments = SegmentUtils.findAllSegments(nposTransaction, "B2");
	            for (Segment segment : b2Segments) {
	                if (segment != null && segment.getSegmentDescription() != null && segment.getSegmentDescription().contains("Type 8")) {
	                	l_id = segment.getSegmentBody().get("Comment Text    Craftsman Club Number or Sears Your Way Rewards");
	                }
	            }
	            
				// 2 if SYWR ID retrieve all items in the basket OK
	            if(l_id!=null) {
                    
	            	// hash l_id with the algorithm specified in the class variables
	            	String hashed = hashLoyaltyId(l_id);
	            	System.out.println(l_id + " : " + hashed);
	            	
	            	//get member variables values from memberVariables collection
					DBObject mbrVariables = memberCollection.findOne(new BasicDBObject("l_id",hashed));
					if(mbrVariables != null) {
						Map<String,Object> memberVariablesMap = new HashMap<String,Object>();
						Iterator<String> mbrVariablesIter = mbrVariables.keySet().iterator();
						while(mbrVariablesIter.hasNext()) {
							String key = mbrVariablesIter.next();
							if(!key.equals("l_id") && !key.equals("_id")) {
								memberVariablesMap.put(variableVidToNameMap.get(key).toUpperCase(), mbrVariables.get(key));
							}
						}
//						System.out.println(" *** Member Variables Map: " + memberVariablesMap);
						
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
						    	if(!simpleDateFormat.parse(((DBObject) collectionChanges.get(key)).get("e").toString()).after(new Date())) {
						    		allChanges.put(key.toUpperCase(), new Change(key.toUpperCase(), ((DBObject) collectionChanges.get(key)).get("v"), simpleDateFormat.parse(((DBObject) collectionChanges.get(key)).get("e").toString())));
						    	}
						    }
						}
		            	
		            
			            Map<String,Change> newChanges = new HashMap<String,Change>();
			            Collection<Segment> c1Segments = SegmentUtils.findAllSegments(nposTransaction, "C1");
			            
						// 3 for each item in the basket find the division OK
			            Collection<String> variableNamesList; 
			            for (Segment segment : c1Segments) {
			            	String div = segment.getSegmentBody().get("Division Number");
	                        System.out.println(" division :" + div );
	
	                        String item = segment.getSegmentBody().get("Item Number");
	                        System.out.println(" item :" + item );
	
	                        RealTimeScoringContext context = createRealTimeScoringContext(segment);
	
	                        variableNamesList = getVariableNamesFromDivAndDivLine(div, item);
	
	
	                        for(String variableName:variableNamesList)
	                        {
	                            System.out.println(" div: " + div + " : " + variableName);
	                            DBObject variableFromVariablesCollection = variablesCollection.findOne(new BasicDBObject("name", variableName));
	                            if (variableFromVariablesCollection != null )System.out.println(" found variable :" + variableName.toUpperCase());
	
	                            try {
	                                //arbitrate between memberVariables and changedMemberVariables to send as previous value
	                                Strategy strategy = (Strategy) Class.forName("analytics.util.strategies."+ variableFromVariablesCollection.get("strategy")).newInstance();
	                                if(allChanges.containsKey(variableName)) {
	                                	context.setPreviousValue(allChanges.get(variableName.toUpperCase()).getValue());
	                                }
	                                else {
	                                	context.setPreviousValue(memberVariablesMap.get(variableName.toUpperCase()));
	                                }
	                                
	                                newChanges.put(variableName, strategy.execute(context)/*this needs to be a strategy*/);
	                            } catch (ClassNotFoundException e) {
	                                e.printStackTrace();
	                            } catch (InstantiationException e) {
	                                e.printStackTrace();
	                            } catch (IllegalAccessException e) {
	                                e.printStackTrace();
	                            }
	
	                        }
			            }
			            
						// 4 if any divisions that affects the HA model - then re-score
			            if(!newChanges.isEmpty()){
//	                        System.out.println("transaction : " + nposTransaction);
//	                        System.out.println(" CHANGES: " + newChanges );
	                        
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
	                            Collection<Integer> models = variableModelsMap.get(changedVariable);
	                            for (Integer modelId: models){
	                                modelsSet.add(modelId);
	                            }
	                        }
	
	
	                        // Score each model in a loop
							BasicDBObject updateRec = new BasicDBObject();
	                        for (Integer modelId:modelsSet)
	                        {
	                        	
	                        	// recalculate score for model
	                        	
//	                            System.out.println("SCORE INPUTS: " + memberVariablesMap + " : " + allChanges + " : " + modelId);
	                            double newScore = 1/(1+ Math.exp(-1*(calcMbrVar(memberVariablesMap, allChanges, modelId)))) * 1000;
	                            System.out.println(l_id + ": " + Double.toString(newScore));
	                            
	                            // FIND THE MIN AND MAX EXPIRATION DATE OF ALL VARIABLE CHANGES FOR CHANGED MODEL SCORE TO WRITE TO SCORE CHANGES COLLECTION
								Date minDate = null;
								Date maxDate = null;
	                            for(String key: allChanges.keySet()) {
	                            	if(variableModelsMap.get(key).contains(modelId)) {
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
	                            
	                            DBObject oldScore = changedMemberScoresCollection.findOne(new BasicDBObject("l_id", hashed));
	                            String message = new StringBuffer().append(l_id).append("-").append(modelId).append("-").append(newChanges).append("-").append(oldScore == null ? "0" : oldScore.get("1")).append("-").append(newScore).toString();
	                            System.out.println(message);
	                            //jedis.publish("score_changes", message);
	                        }
	                        if(updateRec != null) {
	                        	changedMemberScoresCollection.update(new BasicDBObject("l_id", hashed), new BasicDBObject("$set", updateRec), true, false);
	                        }
			            }
                    }
		            else {
		            	return;
		            }
	            }
	            else {
	            	return;
	            }
		            
		            //StringBuffer saleInfo = new StringBuffer().append(zip).append(':').append(sywrCardUsed).append(':').append(amount);
	
		            //if (zip != null && zip != 0)
		                //jedis.publish("sale_info", saleInfo.toString());

	        } catch (JMSException e) {
	            e.printStackTrace();
	        } catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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

    private Collection<String> getVariableNamesFromDivAndDivLine(String div, String item) {
        DBObject line = divLnItmCollection.findOne(new BasicDBObjectBuilder().append("d", div).append("i", item).get());

        DBCursor variables = divLnVariableCollection.find(new BasicDBObject("d",div));

        if (variables != null)
        {
            System.out.println(" variables :" + variables.length() );

        }


        Collection<String> variableNamesList = new ArrayList<String>();

        for(DBObject variable:variables)
        {
            variableNamesList.add(variable.get("v").toString().trim().toUpperCase());
        }

        DBCursor variablesAtDivLine = divLnVariableCollection.find(new BasicDBObject("d",div+line));

        for(DBObject variable:variablesAtDivLine)
        {
            variableNamesList.add(variable.get("v").toString().trim().toUpperCase());
        }
        return variableNamesList;
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
			System.out.println("changed variable: " + var.getName().toUpperCase() + "  value: " + changedValue);
		}
		else {
			return 0;
		}
		return changedValue;
	}
	
}
