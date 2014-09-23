package analytics.integration;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class GenericScoreCheckBolt extends BaseRichBolt{
	Map<String,Object> expected;
	static Map<String,Object>  actual = new HashMap<String, Object>();
	
	public GenericScoreCheckBolt(Map<String,Object> expected) {
		this.expected = expected;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {		
	}

	@Override
	public void execute(Tuple input) {
		for(String key:expected.keySet()){
			if(expected.get(key) instanceof Double){
				actual.put(key,  input.getDoubleByField(key));
			}
			else
				actual.put(key, input.getStringByField(key));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
	}

	public static Map<String,Object> getActualResults(){
		return actual;
		
	}

}
