/**
 * 
 */
package metascale.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ibm.jms.JMSMessage;
import com.mongodb.*;
import metascale.util.Variable;
import org.apache.commons.lang3.ArrayUtils;
import redis.clients.jedis.Jedis;
import shc.npos.segments.Segment;
import shc.npos.util.SegmentUtils;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
	    db.authenticate("rtsw", "$core123".toCharArray());
        memberCollection = db.getCollection("memberVariables");
        modelCollection = db.getCollection("modelVariables");
        memberScoreCollection = db.getCollection("memberScore");
        jedis = new Jedis("151.149.116.48");

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
	            	
	            
		            Map changes = new HashMap();
		            Collection<Segment> c1Segments = SegmentUtils.findAllSegments(nposTransaction, "C1");
		            
					// 3 for each item in the basket find the division OK
		            for (Segment segment : c1Segments) {
		            	String div = segment.getSegmentBody().get("Division Number");
		            	if(ArrayUtils.contains(new String[]{"033","041","043","045"}, div)) {
		            		changes.put("srs_mapp_days_since_last", 0);
		            	}
		            	if(ArrayUtils.contains(new String[]{"020","022","026","032","042","046"}, div)) {
		            		changes.put("srs_appliance_days_since_last", 0);
		            	}
		            	if(ArrayUtils.contains(new String[]{"046"}, div)) {
		            		changes.put("ha_refrig_0_30_purch_flg", 1);
		            	}
		            	
		            	
		            }
		            
					// 4 if any divisions that affects the HA model - then re-score
		            if(!changes.isEmpty()){
                        //System.out.println("transaction : " + nposTransaction);
                        System.out.println(" Changes: " + changes );
		            	//double mbrVar = calcMbrVar(changes, 1, Long.parseLong(l_id));
		            	double newScore = 1/(1+ Math.exp(-1*(calcMbrVar(changes, 1, Long.parseLong(l_id))))) * 1000;
		            	System.out.println(l_id + ": " + Double.toString(newScore));

                        BasicDBObject queryMbr = new BasicDBObject("l_id", Long.valueOf(l_id));
                        DBObject oldScore = memberScoreCollection.findOne(queryMbr);
//                        if (oldScore == null)
//                        {
//                            memberScoreCollection.insert(new BasicDBObjectBuilder().append("l_id", l_id).append("1", String.valueOf(newScore)).get());
//                        }
//                        else
//                        {
//                            memberScoreCollection.update(oldScore, new BasicDBObjectBuilder().append("l_id", l_id).append("1", String.valueOf(newScore)).get());
//                        }
                        jedis.publish("score_changes", new StringBuffer().append(l_id).append("-").append(changes).append("-").append(oldScore == null ? "0" : oldScore.get("1")).append("-").append(newScore).toString());
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

    double calcMbrVar( Map changes, int modelId, long LID)
    {
	    
        BasicDBObject queryModel = new BasicDBObject("modelId", modelId);
        //System.out.println(modelCollection.findOne(queryModel));
	    DBCursor cursor = modelCollection.find( queryModel );
	     
        BasicDBObject queryMbr = new BasicDBObject("l_id", LID);
        //System.out.println(memberCollection.findOne(queryMbr));
        DBObject member = memberCollection.findOne(queryMbr);
        if (member == null) {
		return 0; // TODO:this needs more thought
	} 
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
		    
		    var.makePojoFromBson( dbo );
		     
//		    System.out.println( var.getName() + ", "
//		    + var.getVid() + ", "
//		    + var.getRealTimeFlag()   + ", "
//		    + var.getType()  + ", "
//		    + var.getStrategy()   + ", "
//		    + var.getCoefficeint() );
//
		    if(  var.getType().equals("Integer")) val = val + ((Integer)calculateVariableValue(member, var, changes, var.getType()) * var.getCoefficeint());
		    else if( var.getType().equals("Double")) val = val + ((Double)calculateVariableValue(member, var, changes, var.getType()) * var.getCoefficeint());
		    else {
		    	val = 0;
		    	break;
		    }
		    
	    }
	    
        return val;

        /*
	    double val = (-3.4817414262759
        	    + (Integer)member.get("redeemer_seg") * 0.2960757192325
        	    + (Integer)member.get("srs_mapp_days_since_last") * 0.0004399620490
        	    + (Integer)member.get("srs_appliance_days_since_last") * -0.0006412329322
        	    + (Integer)member.get("earned_points_24m_cd") * 0.1230495072226
        	    + (Integer)member.get("web_flag_refrig_0_7") * 1.5046153477196
        	    + (Integer)member.get("hdln_fl") * 0.3758939603009
        	    + (Double)member.get("srs_str_appliance_sales3m") * 0.0004695336403
        	    + (Integer)member.get("cnt_emal_camp_12m_ind") * 0.2768082292745
        	    + (Integer)member.get("srs_sears_card_amt_ind") * 0.2948286393805
        	    + (Integer)member.get("HS_ANY_dsl") * -0.0000314858291
        	    + (Integer)member.get("num_yr_rsd") * 0.0170213240149
        	    + (Integer)member.get("HA_ANY_5y") * 0.2690159365066
        	    + (Integer)member.get("sr_any_record_flg") * 1.2280939436473
        	    + (Integer)member.get("ha_refrig_0_30_purch_flg") * 0.9681973501297
        	    + (Integer)member.get("cnt_dm_camp_12_mth_ind") * 0.1019036215871
        	    + (Integer)member.get("net_sales_24m_ind") * -0.0475925281865
        	    + (Double)member.get("srs_str_mapp_sales3m") * 0.0034857097067
        	    + (Double)member.get("BlackFriday_Sales_Index_Srs") * -0.0021117730335
        	    + (Integer)member.get("gift_giver_srs") * -3.2426464544210
        	    + (Integer)member.get("HA_ANY_indi") * 0.1390558536262);
    */

    }

	private Object calculateVariableValue(DBObject member, Variable var, Map changes, String dataType) {
		Object changedVar = changes.get(var.getName());
		if(changedVar == null) {
			changedVar=member.get(var.getVid());
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
