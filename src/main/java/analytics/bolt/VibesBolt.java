package analytics.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.VibesDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.mongodb.DBObject;

public class VibesBolt extends EnvironmentBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(VibesBolt.class);
	private VibesDao vibesDao;
	private MultiCountMetric countMetric;
	private OutputCollector outputCollector;
	 public VibesBolt(String systemProperty){
		 super(systemProperty);
	 }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		vibesDao = new VibesDao();
		initMetrics(context);
	}
	
	void initMetrics(TopologyContext context){
		countMetric = new MultiCountMetric();
		context.registerMetric("custom_metrics", countMetric, 60);
	}


	@Override
	public void execute(Tuple input) {
		LOGGER.info("~~~~~~~~~~Incoming tuple in Vibesbolt: " + input);
		countMetric.scope("incoming_tuples").incr();
		DBObject obj = (DBObject) input.getValueByField("vibesDBObject");
		String l_id = null;
		try {
			l_id = (String) obj.get("l_id");
			LOGGER.info("PROCESSING L_Id: " + l_id );
			
			//TODO .. PROCESS THE LID HERE
			
			vibesDao.updateVibes(l_id);
			countMetric.scope("success_vibes").incr();
			outputCollector.ack(input);
		} catch (Exception e) {
			LOGGER.error("Exception Occured at Vibes Bolt for Lid" + l_id );
			e.printStackTrace();
			countMetric.scope("failure_vibes").incr();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
