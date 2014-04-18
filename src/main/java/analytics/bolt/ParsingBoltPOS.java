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

        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
        //db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
	    db.authenticate("rtsw", "5core123".toCharArray());
        memberCollection = db.getCollection("memberVariables");
        divLnItmCollection = db.getCollection("DivLnItm");
        divLnVariableCollection = db.getCollection("DivLnVariable");

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

		String l_id = null;
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
            	l_id = segment.getSegmentBody().get("Comment Text    Craftsman Club Number or Sears Your Way Rewards");
            }
        }
        
        if(l_id==null) {
        	return;
        }
            
		// 5) HASH LOYALTY ID
    	String hashed = hashLoyaltyId(l_id);
    	System.out.println(l_id + " : " + hashed);
        	
		// 6) FETCH SEGMENT "C1"
        Collection<Segment> c1Segments = SegmentUtils.findAllSegments(nposTransaction, "C1");

		// 7) FOR EACH SUB-SEGMENT IN "C1" FIND DIVISION #, ITEM #, AMOUNT AND FIND LINE FROM DIVISION # + ITEM #
		//    AND PUT INTO LINE ITEM CLASS CONTAINER WITH HASHED LOYALTY ID + ALL TRANSACTION LEVEL DATA
        List<Object> lineItemList = new ArrayList<Object>();
        for (Segment segment : c1Segments) {
        	String div = segment.getSegmentBody().get("Division Number");
            String item = segment.getSegmentBody().get("Item Number");
            String amount = segment.getSegmentBody().get("Selling Amount").trim();
            System.out.println(" division: " + div + " item: " + item + " amount: " + amount);

            TransactionLineItem lineItem = new TransactionLineItem(hashed, div, item);
            if(!lineItem.setLineFromCollection(divLnItmCollection)) {
            	continue;
            }
            if(amount.contains("-")) {
            	continue;
            }
            else {
            	lineItem.setAmount(Double.valueOf(amount)/100);
            }
            // find all variables affected by div-line
			List<String> foundVariablesList = new ArrayList<String>();
            if(divLnVariablesMap.containsKey(lineItem.getDiv()+lineItem.getLine())) {
				for(String var: divLnVariablesMap.get(lineItem.getDiv()+lineItem.getLine())) {
					foundVariablesList.add(var);
				}
				lineItemList.add(lineItem);
			}	
        }
        
        
		// 8) EMIT LINE ITEMS
        if(!lineItemList.isEmpty()) {
        	this.outputCollector.emit(lineItemList);
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
		declarer.declare(new Fields("lineItemList"));
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

}
