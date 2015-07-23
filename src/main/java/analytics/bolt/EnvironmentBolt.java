package analytics.bolt;

import java.util.Map;
import analytics.util.MongoNameConstants;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EnvironmentBolt extends BaseRichBolt {
	protected MultiCountMetric countMetric;
	/*private static final Logger LOGGER = LoggerFactory
			.getLogger(EnvironmentBolt.class);*/
	private String environment;
	
	public EnvironmentBolt() {
	}

	public EnvironmentBolt(String systemProperty) {
		environment = systemProperty;
	}
	
	public void redisCountIncr(String scope){
		countMetric.scope(scope).incr();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.setProperty(MongoNameConstants.IS_PROD, environment);
		
		//initializing the metrics
		countMetric = new MultiCountMetric();
		context.registerMetric("custom_metrics", countMetric, 60);
	
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}


}
