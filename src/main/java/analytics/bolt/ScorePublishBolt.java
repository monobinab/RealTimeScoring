/**
 * 
 */
package analytics.bolt;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.ScoringSingleton;
import analytics.util.dao.MemberScoreDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScorePublishBolt extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ScorePublishBolt.class);
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	final String host;
	final int port;
	final String pattern;

	private JedisPool jedisPool;
	private MemberScoreDao memberScoreDao;
	private MultiCountMetric countMetric;
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
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
        System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
	     initMetrics(context);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(100);
        jedisPool = new JedisPool(poolConfig,host, port, 100);
//		jedis = new Jedis(host, port);
		memberScoreDao = new MemberScoreDao();
		this.outputCollector = collector;
	}
	 void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, Constants.METRICS_INTERVAL);
	    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		countMetric.scope("incoming_tuples").incr();
		// System.out.println(" %%% scorepublishbolt :" + input);
		String l_id = input.getStringByField("l_id");
		String modelId = input.getStringByField("model");
		String modelName = ScoringSingleton.getInstance().getModelName(
				Integer.parseInt(modelId));
		String oldScore = memberScoreDao.getMemberScores(l_id).get(modelId);
		String source = input.getStringByField("source");
		Double newScore = input.getDoubleByField("newScore");
		String minExpiry = input.getStringByField("minExpiry");
		String message = new StringBuffer(l_id).append(",").append(modelName)
				.append(",").append(source)
				.append(",").append(newScore)
				.append(",").append(oldScore).toString();
		// System.out.println(" %%% message : " + message);
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.info("TIME:" + messageID + "-Entering score publish bolt-" + System.currentTimeMillis());
		int retryCount = 0;
		while (retryCount < 5) {
			try {
				Jedis jedis = jedisPool.getResource();
				jedis.publish(pattern, message);
				jedisPool.returnResource(jedis);
				break;
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				try {
					Thread.sleep(200);
				} catch (InterruptedException e1) {
					LOGGER.error(e.getMessage(), e);
					if(retryCount==4)
						outputCollector.fail(input);
				}
				retryCount++;
			}
		}
		countMetric.scope("publish_successful").incr();
		outputCollector.ack(input);
		LOGGER.info("TIME:" + messageID + "-Score publish complete-" + System.currentTimeMillis());
		LOGGER.info("PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + l_id + ", modelId: "+modelId + ", oldScore: "+oldScore +", newScore: "+newScore+", minExpiry: "+minExpiry+": source: " + source);

		//l_id, modelId, oldScore, newScore, minExpiry, source 

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
