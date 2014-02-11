/**
 * 
 */
package metascale.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.*;

import java.lang.reflect.Type;
import java.net.UnknownHostException;
import java.util.Map;

public class ScoringBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    DB db;
    DBCollection coll;
    MongoClient mongoClient;


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
            mongoClient = new MongoClient("151.149.191.228", 27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("real-time-scoring-variables");
        //db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
        coll = db.getCollection("memberVariables");
    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {

        DBObject document = (DBObject) input.getValueByField("document");
        Type type = new TypeToken<Map>() {
        }.getType();
        Map gson =    new Gson().fromJson(document.toString(), type);

        DBObject dbObj = (DBObject) com.mongodb.util.JSON.parse("{}");


        BasicDBObject query = new BasicDBObject( "LYL_ID_NO", Long.valueOf(gson.get("lyl_id_no").toString()));

        DBObject member = coll.findOne(query);

        System.out.println(" memeber found : " + member);


        if (member != null) {
        double score = 1/(1+ Math.exp(-1*(-3.4817414262759
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
                + (Integer)member.get("HA_ANY_indi") * 0.1390558536262))) * 1000;

            System.out.println(" new score!!! : " + score);


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

}
