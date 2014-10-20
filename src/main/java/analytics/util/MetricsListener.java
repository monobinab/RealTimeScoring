package analytics.util;

import java.util.Collection;
import java.util.Map;

import org.json.simple.JSONObject;

import redis.clients.jedis.Jedis;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

public class MetricsListener implements IMetricsConsumer {

	String topologyName;
	Jedis jedis;
	
	@Override
	public void prepare(Map stormConf, Object registrationArgument,
			TopologyContext context, IErrorReporter errorReporter) {
		jedis = new Jedis("10.2.8.175", 11211);
		topologyName = (String) stormConf.get("topology.name");
	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		for (DataPoint dataPoint : dataPoints) {
			if (dataPoint.name.equalsIgnoreCase("custom_metrics")) {
				Map<String, Long> map = (Map<String, Long>) dataPoint.value;
				for (String key : map.keySet()) {
					String redisKey = topologyName+taskInfo.srcComponentId + key;
					long totalCount = jedis.incrBy(redisKey, map.get(key));
					JSONObject jsonObj = new JSONObject();
					jsonObj.put("topologyName", topologyName);
					jsonObj.put("srcComponentId", taskInfo.srcComponentId);
					jsonObj.put("type", key);
					jsonObj.put("valueTotal", map.get(key));
					jsonObj.put("valueAvg", totalCount);
					jedis.publish("metrics_test", jsonObj.toJSONString());
					System.out.println(jsonObj.toJSONString());
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