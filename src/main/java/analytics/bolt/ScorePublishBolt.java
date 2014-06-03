/**
 * 
 */
package analytics.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ScorePublishBolt extends BaseRichBolt {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    final String host;
    final int port;
    final String pattern;


    DB db;
    MongoClient mongoClient;
    DBCollection memberZipCollection;
    DBCollection memberScoreCollection;


    private Map<String,Collection<Integer>> variableModelsMap;
    private Map<String, String> variableVidToNameMap;
    private Map<String,String> modelIdToModelNameMap;

    private Jedis jedis;


    public ScorePublishBolt(String host, int port, String pattern) {
        this.host = host;
        this.port = port;
        this.pattern = pattern;
    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        jedis = new Jedis(host, port);
        this.outputCollector = collector;


        //prepare mongo
        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
        //db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
        db.authenticate("rtsw", "5core123".toCharArray());
        memberZipCollection = db.getCollection("memberZip");
        memberScoreCollection = db.getCollection("memberScore");
        
        modelIdToModelNameMap = new HashMap<String,String>();
        modelIdToModelNameMap.put("34", "S_SCR_HA_ALL");
        modelIdToModelNameMap.put("35", "S_SCR_HA_COOK");
        
    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {

        //System.out.println(" %%% scorepublishbolt :" + input);
        String l_id = input.getStringByField("l_id");
        DBObject row = memberZipCollection.findOne(new BasicDBObject("l_id", l_id));
        DBObject oldScoreResult = memberScoreCollection.findOne(new BasicDBObject("l_id", l_id));
        String modelName = modelIdToModelNameMap.get(input.getStringByField("model"));
        String oldScore = new String();
        
        String modelDescription = new String();
        if(modelName.equals("S_SCR_HA_ALL")) {
        	modelDescription="HA ALL";
        }
        else if (modelName.equals("S_SCR_HA_COOK")) {
        	modelDescription="HA COOKING";
        }
        else {
        	modelDescription="MODEL NOT FOUND";
        }
        
        if(oldScoreResult != null && modelName != null) {
        	oldScore = oldScoreResult.get(modelName).toString();
        }
        else {
        	oldScore = "0";
        }
        
        String dataSource = new String();
        if(input.getStringByField("source").equals("NPOS")) {
        	dataSource = "POS Sales";
        }
        else {
        	dataSource = "Web Browse";
        }
        Object zip = row==null?"00000":row.get("z");
        //System.out.println(" %%% zip zip :" + ObjectUtils.toString(zip));

        if (row != null && StringUtils.isNotEmpty(ObjectUtils.toString(zip)))
            {
                String message = new StringBuffer(l_id).append(",")
                        .append(oldScore).append(",")
                        .append(input.getDoubleByField("newScore")).append(",")
                        .append(modelDescription).append(",")
                        .append(dataSource).append(",")
                        .append(zip.toString()).toString();
                //System.out.println(" %%% message : " + message);

                jedis.publish(pattern, message);
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
