package analytics.bolt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import analytics.jmx.AppMetricsBean;
import analytics.jmx.JMXConnectionManager;
import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheRefreshScheduler;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EnvironmentBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	protected MultiCountMetric countMetric;
	/*private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentBolt.class);*/
	private String environment;
	//protected AppMetricsBean appMetricsBean;
	//protected AtomicInteger atomicInteger;
	
	public EnvironmentBolt() {}

	public EnvironmentBolt(String systemProperty) {
	   environment = systemProperty;
	   //CacheRefreshScheduler.getInstance().startScheduler();
	   /**
	   JMXConnectionManager jmxConnectionManager = JMXConnectionManager.getInstance();
        if(jmxConnectionManager != null){
        	if(appMetricsBean == null && atomicInteger == null){
        		appMetricsBean = jmxConnectionManager.getAppMetricsBean();
        		atomicInteger = jmxConnectionManager.getAtomicIntegerInstance();
        	}
        }*/
	}
	
	public void redisCountIncr(String scope){
		countMetric.scope(scope).incr();
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.setProperty(MongoNameConstants.IS_PROD, environment);
		
		//initializing the metrics
		countMetric = new MultiCountMetric();
		context.registerMetric("custom_metrics", countMetric, 60);
		CacheRefreshScheduler.getInstance().startScheduler();
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
