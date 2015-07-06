package analytics.bolt;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import analytics.util.dao.MemberUUIDDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class SignalBolt extends EnvironmentBolt{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SignalBolt.class);
	private OutputCollector outputCollector;
	private MemberUUIDDao memberUUIDDao;
	private String redisHost;
	private int redisPort;
	
	public SignalBolt(String systemProperty, String redisHost, int redisPort) {
		super(systemProperty);
		this.redisHost = redisHost;
		this.redisPort = redisPort;
	}

	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		LOGGER.debug("Preparing SignalRedisBolt");
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		memberUUIDDao = new MemberUUIDDao();
	}
	
	@Override
	public void execute(Tuple input) {
		
		try{
				if(input.contains("type") && input.getStringByField("type").equalsIgnoreCase("BrowseProduct")){
				Jedis jedis = null;
				redisCountIncr("incoming_tuples");
				List<String> l_Ids = memberUUIDDao.getLoyaltyIdsFromUUID((String) input.getValueByField("uuid"));
				for(String loyaltyId : l_Ids){
					if(redisHost!=null){
						jedis = new Jedis(redisHost, redisPort, 1800);
						jedis.connect();
						String loyId = "signal:"+loyaltyId;
						if(!(jedis.exists(loyaltyId))){
							jedis.rpush(loyId, System.currentTimeMillis()+"");
							LOGGER.info(loyaltyId + " persisted to redis");
						}
						jedis.rpush(loyId, input.getStringByField("products"));
						LOGGER.info(loyaltyId + " appended to redis with pids " + input.getStringByField("products"));
						redisCountIncr("lids_to_redis");
						jedis.disconnect();
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
			LOGGER.error("Exception in SignalRedisBolt ", e);
		}
			outputCollector.ack(input);
	}
}