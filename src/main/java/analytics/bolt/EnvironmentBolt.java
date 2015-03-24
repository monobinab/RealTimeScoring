package analytics.bolt;

import java.util.Map;

import analytics.util.MongoNameConstants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EnvironmentBolt extends BaseRichBolt{
private String environment;

public EnvironmentBolt(){
	
}
	public EnvironmentBolt(String systemProperty) {
		environment = systemProperty;
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
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
