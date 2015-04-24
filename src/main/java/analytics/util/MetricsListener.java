package analytics.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

public class MetricsListener implements IMetricsConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsListener.class);
	private String topologyName;
	private JedisPool jedisPool;
	Properties prop = new Properties();
	@Override
	public void prepare(Map stormConf, Object registrationArgument,
			TopologyContext context, IErrorReporter errorReporter) {
		String isProd = (String) registrationArgument;
		LOGGER.info("~~~~~~~~~system property in MetricsListener prepare~~~~~" + isProd);
		try {
			prop.load(MetricsListener.class.getClassLoader().getResourceAsStream("resources/redis_server_metrics.properties"));
		} catch (IOException e) {
			LOGGER.error("Unable to initialize metrics");
		}
		int port = Integer.parseInt(prop.getProperty("port"));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(100);
		if("PROD".equals(isProd)){
	        jedisPool = new JedisPool(poolConfig,prop.getProperty("prod"), port, 1000);
		}
		else if("QA".equals(isProd)){
	        jedisPool = new JedisPool(poolConfig,prop.getProperty("qa"), port, 1000);
		}
		else if("LOCAL".equals(isProd)){
	        jedisPool = new JedisPool(poolConfig,prop.getProperty("qa"), port, 1000);
		}
		topologyName = (String) stormConf.get("metrics_topology");
		//System.out.println("~~~~~~~~~redis taken in MetricsListener~~~~~" + jedisPool.getResource().getClient().getHost());
		
	//	LOGGER.info("~~~~~~~~~redis taken in MetricsListener~~~~~" + jedisPool.getResource().getClient().getHost());
	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		for (DataPoint dataPoint : dataPoints) {
			if (dataPoint.name.equalsIgnoreCase("custom_metrics")) {
				Map<String, Long> map = (Map<String, Long>) dataPoint.value;
				for (String key : map.keySet()) {
					try{
					String redisKey = topologyName+":"+taskInfo.srcComponentId +":"+ key;
					Jedis jedis = jedisPool.getResource();
					long totalCount = jedis.incrBy(redisKey, map.get(key));
					JSONObject jsonObj = new JSONObject();
					jsonObj.put("topologyName", topologyName);
					jsonObj.put("srcComponentId", taskInfo.srcComponentId);
					jsonObj.put("type", key);
					JSONObject display = new JSONObject();
					display.put("valueAvg", map.get(key));
					display.put("valueTotal", totalCount);
					jsonObj.put("display", display);
					jedis.publish("metrics", jsonObj.toJSONString());
					jedisPool.returnResource(jedis);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
				}
		}
		/*
		 * {
		 * format
    "topologyName": "AAMTopology",
    "srcComponentId": "ParseAAMFeeds",
    "type": "emitting",
    "valueTotal": "1029",
    "valueAvg": "132"
			}

		 */
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}