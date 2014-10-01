package analytics.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberZipDao;
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

	private Jedis jedis;
	private MemberZipDao memberZipDao;
	
	public MemberPublishBolt(String host, int port, String pattern) {
		this.host = host;
		this.port = port;
		this.pattern = pattern;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
        System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
		jedis = new Jedis(host, port);
		memberZipDao = new MemberZipDao();
		this.outputCollector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		String l_id = input.getStringByField("l_id");
		String source = input.getStringByField("source");
		String zip = memberZipDao.getMemberZip(l_id);

		String message = new StringBuffer(l_id).append(",").append(zip)
				.append(",").append(source).toString();

		int retryCount = 0;
		while (retryCount < 5) {
			try {
				jedis.publish(pattern, message);
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
		outputCollector.ack(input);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
