package analytics.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.dao.DCDao;
import analytics.util.dao.MemberDCDao;
import analytics.util.dao.MemberScoreDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EnvironmentBolt extends BaseRichBolt {
	protected MultiCountMetric countMetric;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(EnvironmentBolt.class);
	private String environment;
	protected DCDao dc;
	protected MemberDCDao memberDCDao;
	protected MemberScoreDao memberScoreDao;

	public EnvironmentBolt() {
	}

	public EnvironmentBolt(String systemProperty) {
		environment = systemProperty;
	}
	
	public void redisCountIncr(String scope){
		countMetric.scope(scope).incr();
		LOGGER.info("~~~~in redisCoutnIncr of Environment bolt~~");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.setProperty(MongoNameConstants.IS_PROD, environment);
		//initMetrics(context);
		countMetric = new MultiCountMetric();
		context.registerMetric("custom_metrics", countMetric, 60);
		memberDCDao = new MemberDCDao();
		dc = new DCDao();
		memberScoreDao = new MemberScoreDao();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

/*	public void systemPropertySet() {
		System.setProperty(MongoNameConstants.IS_PROD, environment);
	}*/

/*	void initMetrics(TopologyContext context) {
		countMetric = new MultiCountMetric();
		context.registerMetric("custom_metrics", countMetric, 60);
	}*/

}
