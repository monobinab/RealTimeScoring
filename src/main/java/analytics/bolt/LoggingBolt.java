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

public class LoggingBolt extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(LoggingBolt.class);
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	private MultiCountMetric countMetric;
	private MemberScoreDao memberScoreDao;
	public LoggingBolt() {
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
	    initMetrics(context);
		this.outputCollector = collector;
		memberScoreDao = new MemberScoreDao();
        System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
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
		//System.out.println(" %%% scorepublishbolt :" + input);
		String l_id = input.getStringByField("l_id");
		String modelId = input.getStringByField("model");
		String oldScore = memberScoreDao.getMemberScores(l_id).get(modelId);
		String source = input.getStringByField("source");
		Double newScore = input.getDoubleByField("newScore");
		String minExpiry = input.getStringByField("minExpiry");
		String maxExpiry = input.getStringByField("maxExpiry");

		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.info("TIME:" + messageID + "-Entering logging bolt-" + System.currentTimeMillis());
		LOGGER.info("PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + l_id + ", modelId: "+modelId + ", oldScore: "+oldScore +", newScore: "+newScore+", minExpiry: "+minExpiry+", maxExpiry: "+maxExpiry+", source: " + source);
		//System.out.println("PERSIST: " + new Date() + ": Topology: Changes Scores : lid: " + l_id + ", modelId: "+modelId + ", oldScore: "+oldScore +", newScore: "+newScore+", minExpiry: "+minExpiry+", maxExpiry: "+maxExpiry+", source: " + source);

		countMetric.scope("score_logged").incr();
		outputCollector.ack(input);	}

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
