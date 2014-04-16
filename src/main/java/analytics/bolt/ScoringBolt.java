/**
 * 
 */
package analytics.bolt;

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
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    DBCollection memberCollection;
    DBCollection memberScoreCollection;
    DBCollection variablesCollection;
    DBCollection divLnItmCollection;
    DBCollection divLnVariableCollection;

    private Map<String,Collection<Integer>> variableModelsMap;

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
                     addModel(model, variableName, modelIds);
                 }
                 else
                 {
                     Collection<Integer> modelIds = variableModelsMap.get(variableName);
                     addModel(model, variableName, modelIds);
                 }

             }
        }

        System.out.println(" variablesModelMap: " + variableModelsMap);




        //jedis = new Jedis("151.149.116.48");

    }

    private void addModel(DBObject model, String variableName, Collection<Integer> modelIds) {
        modelIds.add(Integer.valueOf(model.get("modelId").toString()));
        variableModelsMap.put(variableName, modelIds);
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
			// 5 store new score in mongodb  PENDING
			// 6 store the score in redis    PENDING

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
	            	
	            
		            Map<String,Object> changes = new HashMap<String,Object>();
		            Collection<Segment> c1Segments = SegmentUtils.findAllSegments(nposTransaction, "C1");
		            
					// 3 for each item in the basket find the division OK
		            for (Segment segment : c1Segments) {
		            	String div = segment.getSegmentBody().get("Division Number");
                        System.out.println(" division :" + div );

                        String item = segment.getSegmentBody().get("Item Number");
                        System.out.println(" item :" + item );

                        RealTimeScoringContext context = createRealTimeScoringContext(segment);

                        Collection<String> variableNamesList = getVariableNamesFromDivAndDivLine(div, item);


                        for(String variableName:variableNamesList)
                        {
                            System.out.println(" div :" + div + ":" + variableName);
                            DBObject variableFromVariablesCollection = variablesCollection.findOne(new BasicDBObject("name", variableName));
                            if (variableFromVariablesCollection != null )System.out.println(" found variable :" + variableName);

                            try {
                                Strategy strategy = (Strategy) Class.forName("analytics.util.strategies."+ variableFromVariablesCollection.get("strategy")).newInstance();
                                changes.put(variableName, strategy.execute(context)/*this needs to be a strategy*/);

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
		            if(!changes.isEmpty()){
                        //System.out.println("transaction : " + nposTransaction);
                        System.out.println(" Changes: " + changes );
		            	//double mbrVar = calcMbrVar(changes, 1, Long.parseLong(l_id));


		            	/*

                        // find all the models that are affected by these changes
                        Set<Integer> modelsSet = new HashSet<Integer>();
                        for(String changedVariable:changes.keySet())
                        {
                            Collection<Integer> models = variableModelsMap.get(changedVariable);
                            for (Integer modelId: models){
                                modelsSet.add(modelId);
                            }
                        }


                        // Score each model in a loop

                        for (Integer modelId:modelsSet)
                        {
                            double newScore = 1/(1+ Math.exp(-1*(calcMbrVar(changes, modelId, Long.parseLong(l_id))))) * 1000;
                            System.out.println(l_id + ": " + Double.toString(newScore));

                            MessageDigest digest = null;
                            try {
                                digest = MessageDigest.getInstance("MD5");
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                            digest.update(l_id.toString().getBytes());
                            BasicDBObject queryMbr = new BasicDBObject("l_id", new BigInteger(1, digest.digest()).toString(16));
                            DBObject oldScore = memberScoreCollection.findOne(queryMbr);
    //                        if (oldScore == null)
    //                        {
    //                            memberScoreCollection.insert(new BasicDBObjectBuilder().append("l_id", l_id).append("1", String.valueOf(newScore)).get());
    //                        }
    //                        else
    //                        {
    //                            memberScoreCollection.update(oldScore, new BasicDBObjectBuilder().append("l_id", l_id).append("1", String.valueOf(newScore)).get());
    //                        }
                            String message = new StringBuffer().append(l_id).append("-").append(modelId).append("-").append(changes).append("-").append(oldScore == null ? "0" : oldScore.get("1")).append("-").append(newScore).toString();
                            System.out.println(message);
                            //jedis.publish("score_changes", message);
                        }                                                     */
                    }
		            else {
		            	return;
		            }
		            
		            //StringBuffer saleInfo = new StringBuffer().append(zip).append(':').append(sywrCardUsed).append(':').append(amount);
	
		            //if (zip != null && zip != 0)
		                //jedis.publish("sale_info", saleInfo.toString());
	            }

	        } catch (JMSException e) {
	            e.printStackTrace();
	        }

			

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
        context.setPreviousValue(0d); //TODO: this has to come from the member variable
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

    double calcMbrVar( Map<String,Object> changes, int modelId, long LID)
    {
	    
        BasicDBObject queryModel = new BasicDBObject("modelId", modelId);
        //System.out.println(modelCollection.findOne(queryModel));
	    DBCursor cursor = modelCollection.find( queryModel );

        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        digest.update(String.valueOf(LID).getBytes());
        BasicDBObject queryMbr = new BasicDBObject("l_id", new BigInteger(1, digest.digest()).toString(16));
        //BasicDBObject queryMbr = new BasicDBObject("l_id", LID);
        //System.out.println(memberCollection.findOne(queryMbr));
        DBObject member = memberCollection.findOne(queryMbr);
        if (member == null) {
		    return 0; // TODO:this needs more thought
	    }
	    System.out.println("member :" + member);
        DBObject model = null;
	    
	    while( cursor.hasNext() )
	    	model = ( BasicDBObject ) cursor.next();
	    
	    
	    //System.out.println( model.get( "modelId" ) + ": " + model.get( "constant" ).toString());
	    
	    double val = (Double) model.get( "constant" );
	    
	    BasicDBList variable = ( BasicDBList ) model.get( "variable" );
	    Variable var = new Variable();
	    
	    for( Iterator< Object > it = variable.iterator(); it.hasNext(); )
	    {
	    	BasicDBObject dbo     = ( BasicDBObject ) it.next();
	    	BasicDBObject queryVariableId = new BasicDBObject("name", dbo.get("name").toString().toUpperCase());
		    
	    	DBObject variableFromVariablesCollection = this.variablesCollection.findOne(queryVariableId);
	    	System.out.println(variableFromVariablesCollection);

            var.setVid(variableFromVariablesCollection.get("VID").toString());
            var.makePojoFromBson( dbo );


		    System.out.println( var.getName() + ", "
		    + var.getRealTimeFlag()   + ", "
		    + var.getType()  + ", "
		    + var.getStrategy()   + ", "
		    + var.getCoefficeint() +", "
            + var.getVid());

		    if(  var.getType().equals("Integer")) val = val + ((Integer)calculateVariableValue(member, var, changes, var.getType()) * var.getCoefficeint());
		    else if( var.getType().equals("Double")) val = val + ((Double)calculateVariableValue(member, var, changes, var.getType()) * var.getCoefficeint());
		    else {
		    	val = 0;
		    	break;
		    }
		    
	    }
	    
        return val;

    }

	private Object calculateVariableValue(DBObject memberVariable, Variable var, Map changes, String dataType) {
		Object changedVar = changes.get(var.getName());
		if(changedVar == null) {
			changedVar=memberVariable.get(var.getVid());
		}
		else{
			if(dataType.equals("Integer")) {
				changedVar=Integer.parseInt(changedVar.toString());
			}
			else {
				changedVar=Double.parseDouble(changedVar.toString());
			}
		System.out.println("changed variable value: " + changedVar.toString());
		}
		return changedVar;
	}

	
}
