package analytics.integration;

import java.util.Map;

import junit.framework.Assert;
import analytics.util.ScoringSingleton;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class GenericScoreCheckBolt extends BaseRichBolt{
	Map<String,Object> expected;
	public GenericScoreCheckBolt(Map<String,Object> values) {
		expected=values;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {		
	}

	@Override
	public void execute(Tuple input) {
		System.out.println(input);
		for(String key:expected.keySet()){
			if(expected.get(key) instanceof Double)
				Assert.assertEquals(expected.get(key), input.getDoubleByField(key));
			else
				Assert.assertEquals(expected.get(key), input.getStringByField(key));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
