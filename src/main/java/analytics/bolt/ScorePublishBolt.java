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
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.util.Collection;
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


    private Map<String,Collection<Integer>> variableModelsMap;
    private Map<String, String> variableVidToNameMap;

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
    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {


        String l_id = input.getStringByField("l_id");
        DBObject row = memberZipCollection.findOne(new BasicDBObject("l_id", l_id));
            if (row != null && StringUtils.isNotEmpty(row.get("zip").toString()))
            {
                jedis.publish(pattern, new StringBuffer(l_id).append(",")
                        .append(input.getStringByField("oldScore")).append(",")
                        .append(input.getStringByField("newScore")).append(",")
                        .append(input.getStringByField("model")).append(",")
                        .append(input.getStringByField("source")).append(",")
                        .append(row.get("zip").toString()).toString());
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
