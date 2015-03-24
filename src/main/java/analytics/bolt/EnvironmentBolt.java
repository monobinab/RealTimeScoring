package analytics.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EnvironmentBolt extends BaseRichBolt{
private String environment;
private static final Logger LOGGER = LoggerFactory
.getLogger(ParsingBoltOccassion.class);

public EnvironmentBolt(){
	
}
	public EnvironmentBolt(String systemProperty) {
		environment = systemProperty;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		LOGGER.info("~~~~~~~~~~~~~~~ENVIRONMENT BOLT~~~~~~~: " + System.getProperty(MongoNameConstants.IS_PROD));
		System.setProperty(MongoNameConstants.IS_PROD, environment);
		
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
