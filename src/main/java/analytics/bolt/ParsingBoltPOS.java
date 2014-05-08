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
import com.ibm.jms.JMSMessage;
import com.mongodb.*;

import redis.clients.jedis.Jedis;
import shc.npos.segments.Segment;
import shc.npos.util.SegmentUtils;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import java.security.SignatureException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;


public class ParsingBoltPOS extends BaseRichBolt {
	/**
	 * Created by Rock Wasserman 4/18/2014
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    MongoClient mongoClient;
    DBCollection memberCollection;
    DBCollection divLnItmCollection;
    DBCollection divLnVariableCollection;

    private Map<String,Collection<String>> divLnVariablesMap;

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

    public void setDivLnItmCollection(DBCollection divLnItmCollection) {
        this.divLnItmCollection = divLnItmCollection;
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

        System.out.println("PREPARING PARSING POS BOLT");
        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
        //db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
	    db.authenticate("rtsw", "5core123".toCharArray());
        memberCollection = db.getCollection("memberVariables");
        divLnItmCollection = db.getCollection("divLnItm");
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

		String lyl_id_no = null;
		JMSMessage document = (JMSMessage) input.getValueByField("npos");
			
		// 1) FETCH SEGMENT "B1"
		// 2) TEST IF TRANSACTION TYPE CODE IS = 1 (RETURN IF FALSE)
		// 3) FETCH SEGMENT "B2"
		// 4) TEST IF TRANSACTION IS A MEMBER TRANSACTION (IF NOT RETURN)
		// 5) HASH LOYALTY ID
		// 6) FETCH SEGMENT "C1"
		// 7) FOR EACH SUB-SEGMENT IN "C1" FIND DIVISION #, ITEM #, AMOUNT AND FIND LINE FROM DIVISION # + ITEM #
		//    AND PUT INTO LINE ITEM CLASS CONTAINER WITH HASHED LOYALTY ID + ALL TRANSACTION LEVEL DATA
		// 8) EMIT LINE ITEMS

        	
		System.out.println("PARSING NPOS DOCUMENT");
		
		// 1) FETCH SEGMENT "B1"
    	
        String nposTransaction = null;
		try {
			nposTransaction = ((TextMessage) document).getText();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(nposTransaction == null) {
			return;
		}
		
        Collection<Segment> saleSegments = SegmentUtils.findAllSegments(nposTransaction, "B1");

		// 2) TEST IF TRANSACTION TYPE CODE IS = 1 (RETURN IF FALSE)
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
        
		// 3) FETCH SEGMENT "B2"
        Collection<Segment> b2Segments = SegmentUtils.findAllSegments(nposTransaction, "B2");

		// 4) TEST IF TRANSACTION IS A MEMBER TRANSACTION (IF NOT RETURN)
        for (Segment segment : b2Segments) {
            if (segment != null && segment.getSegmentDescription() != null && segment.getSegmentDescription().contains("Type 8")) {
            	lyl_id_no = segment.getSegmentBody().get("Comment Text    Craftsman Club Number or Sears Your Way Rewards");
            }
        }
        
        if(lyl_id_no==null) {
        	return;
        }
            
		// 5) HASH LOYALTY ID
    	String l_id = hashLoyaltyId(lyl_id_no);
    	System.out.println(lyl_id_no + " : " + l_id);
        	
		// 6) FETCH SEGMENT "C1"
        Collection<Segment> c1Segments = SegmentUtils.findAllSegments(nposTransaction, "C1");

		// 7) FOR EACH SUB-SEGMENT IN "C1" FIND DIVISION #, ITEM #, AMOUNT AND FIND LINE FROM DIVISION # + ITEM #
		//    AND PUT INTO LINE ITEM CLASS CONTAINER WITH HASHED LOYALTY ID + ALL TRANSACTION LEVEL DATA
        Collection<TransactionLineItem> lineItemList = new ArrayList<TransactionLineItem>();
        for (Segment segment : c1Segments) {
        	String div = segment.getSegmentBody().get("Division Number");
            String item = segment.getSegmentBody().get("Item Number");
            String amount = segment.getSegmentBody().get("Selling Amount").trim();
            //System.out.println(" division: " + div + " item: " + item + " amount: " + amount);
            
            String line = getLineFromCollection(div,item);

            
            if(line==null) {
            	continue;
            }
            if(amount.contains("-")) {
            	continue;
            }
            else {
            	TransactionLineItem lineItem = new TransactionLineItem(l_id, div, item, line, Double.valueOf(amount)/100);

            	// find all variables affected by div-line
				List<String> foundVariablesList = new ArrayList<String>();
	            if(divLnVariablesMap.containsKey(lineItem.getDiv()+lineItem.getLine()) || divLnVariablesMap.containsKey(lineItem.getDiv())) {
					Collection<String> divVariableCollection = divLnVariablesMap.get(lineItem.getDiv());
					Collection<String> divLnVariableCollection = divLnVariablesMap.get(lineItem.getDiv()+lineItem.getLine());
					if(divVariableCollection!=null) {
						for(String var: divVariableCollection) {
							foundVariablesList.add(var);
						}
					}
					if(divLnVariableCollection!=null) {
						for(String var: divLnVariableCollection) {
							foundVariablesList.add(var);
						}
					}
					lineItem.setVariableList(foundVariablesList);
					System.out.println("  div: " + lineItem.getDiv() 
							+ " ln: " + lineItem.getLine() 
							+ " itm: " + lineItem.getItem() 
							+ " amt: " + lineItem.getAmount()
							+ " variable list: " + lineItem.getVariableList());
					lineItemList.add(lineItem);
				}	
            }
        }
        
        System.out.println("list size: " + lineItemList.size());
        if(lineItemList.size()>0){
	        List<Object> lineItemAsJsonString = new ArrayList<Object>();
	        lineItemAsJsonString.add(createJsonFromLineItemList(lineItemList));

	        System.out.println(" *** parsing bolt emitting: " + lineItemAsJsonString.toString());
	        
			// 8) EMIT LINE ITEMS
	        if(lineItemAsJsonString!=null && !lineItemAsJsonString.isEmpty()) {
	        	this.outputCollector.emit(lineItemAsJsonString);
	        }
        }
    }

    private Object createJsonFromLineItemList(Collection<TransactionLineItem> lineItemCollection) {
		// Create string in JSON format to emit
    	/*
    	String transLineItemListString = new String();
    	
    	for(TransactionLineItem itm: lineItemCollection) {
			transLineItemListString = transLineItemListString 
					+ "{\"hashed\":\"" + itm.getHashed() + "\""
					+ ",\"div\":\"" + itm.getDiv() + "\""
					+ ",\"ln\":\"" + itm.getLine() + "\""
					+ ",\"itm\":\"" + itm.getItem() + "\""
					+ ",\"amt\":" + itm.getAmount();
			
			int variableCount = 0;
			for(String var: itm.getVariableList()) {
				if(++variableCount==1) {
					transLineItemListString = transLineItemListString 
							+ "[{\"var\":\"" + var + "\"}";
				}
				else {
					transLineItemListString = transLineItemListString 
							+ ",{\"var\":\"" + var + "\"}";
				}
			}
    	}
		transLineItemListString = transLineItemListString + "}]}";
    	System.out.println("concatenation:  " + transLineItemListString);
    	
    	*/
    	Gson gson = new Gson();
    	Type transLineItemType = new TypeToken<Collection<TransactionLineItem>>() {}.getType();
    	String transLineItemListString = gson.toJson(lineItemCollection, transLineItemType);
    	
		return transLineItemListString;
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
		declarer.declare(new Fields("lineItemAsJsonString"));
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
	
	public String getLineFromCollection(String div, String item) {
		//System.out.println("searching for line");
		
		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put("d", div);
		queryLine.put("i", item);
		
		DBObject divLnItm = divLnItmCollection.findOne(queryLine);
		
		if(divLnItm==null || divLnItm.keySet()==null || divLnItm.keySet().isEmpty()) {
			return null;
		}
		String line = divLnItm.get("l").toString();
		//System.out.println("  found line: " + line);
		return line;
	}
	

}
