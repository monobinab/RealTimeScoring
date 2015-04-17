package analytics.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.util.ResponsysUtil;
import analytics.util.SecurityUtils;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ResponsysUnknownCallsBolt  extends EnvironmentBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponsysUnknownCallsBolt.class);
	
	private MultiCountMetric countMetric;
	private OutputCollector outputCollector;
	private ResponsysUtil responsysUtil;
	
	public ResponsysUnknownCallsBolt(String systemProperty) {
		super(systemProperty);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		responsysUtil = new ResponsysUtil();
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(100);
        
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		String lyl_id_no = null; 
		
		try {
			
			if(input != null && input.contains("lyl_id_no")){
				lyl_id_no = input.getString(0);
				String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no);
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
				
				
				
					//Send response for every new tag scored
					//length -1 because the last element would be the datestring set in the parsing bolt.
			    responsysUtil.getResponseXMLServiceResult(scoreInfoJsonString,lyl_id_no);
			    countMetric.scope("responses").incr();
				
				/*getResponseServiceResult(scoreInfoJsonString,lyl_id_no);
				countMetric.scope("responses").incr();*/

				/*//Delete the lid from redis after processing
				jedis = jedisPool.getResource();
				jedis.del("Responses:"+lId_Date);
				jedisPool.returnResource(jedis);*/
			}
			outputCollector.ack(input);
			
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("responses_failed").incr();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
}
