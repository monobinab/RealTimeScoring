package analytics.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberZipDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MemberPublishBolt extends BaseRichBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberPublishBolt.class);
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	final String host;
	final int port;
	final String pattern;

	private JedisPool jedisPool;
	private MemberZipDao memberZipDao;
	private MultiCountMetric countMetric;
	public MemberPublishBolt(String host, int port, String pattern) {
		this.host = host;
		this.port = port;
		this.pattern = pattern;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
        System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
        initMetrics(context);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(100);
        jedisPool = new JedisPool(poolConfig,host, port, 100);
        memberZipDao = new MemberZipDao();
		this.outputCollector = collector;
		
	}
	 void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }
	@Override
	public void execute(Tuple input) {
		countMetric.scope("incoming_tuples").incr();
		String l_id = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String zip = memberZipDao.getMemberZip(l_id);

		String message = new StringBuffer(l_id).append(",").append(zip)
				.append(",").append(source).toString();
		String messageID = "";
		if (input.contains("messageID")) {
			messageID = input.getStringByField("messageID");
		}
		LOGGER.info("TIME:" + messageID + "-Entering member publish bolt-" + System.currentTimeMillis());
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
		LOGGER.info("TIME:" + messageID + "-Member publish successful-" + System.currentTimeMillis());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
