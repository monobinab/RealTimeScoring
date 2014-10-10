package analytics.util;

import java.util.Collection;
import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;

public class MetricsListener implements IMetricsConsumer {

	Jedis jedis;
	@Override
	public void prepare(Map stormConf, Object registrationArgument,
			TopologyContext context, IErrorReporter errorReporter) {
		jedis = new Jedis("10.2.8.175", 11211);

	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo,
			Collection<DataPoint> dataPoints) {
		String compId = taskInfo.srcWorkerHost  + ":" + taskInfo.srcWorkerPort + " - " + taskInfo.srcComponentId + " - " + taskInfo.timestamp + " : ";
		for (DataPoint dataPoint : dataPoints) {
			if (dataPoint.name.equalsIgnoreCase("custom_metrics")) {
				Map<String, Object> map = (Map<String, Object>) dataPoint.value;
				for (String key : map.keySet()) {
					jedis.publish("metrics", compId + key + " - " + map.get(key));
				}
				}
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}