package analytics.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import analytics.util.JsonUtils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TestingSpout extends BaseRichSpout{

	private SpoutOutputCollector outputCollector;
	public TestingSpout(){
		
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.outputCollector = collector;
		
	}
	

	@Override
	public void nextTuple() {
		
		Map<String, String> varaibleValueMap = new HashMap<String, String>();
		varaibleValueMap.put("BLACKOUT_S_TV", "0.0");
		varaibleValueMap.put("BLACKOUT_S_LG_TRIM_EDG", "0.0");
		varaibleValueMap.put("BLACKOUT_HA_COOK", "0.0");
		varaibleValueMap.put("BLACKOUT_MATTRESS", "0.0");
		varaibleValueMap.put("BLACKOUT_WASH_DRY", "0.0");
		
		List<Object> listToEmit = new ArrayList<Object>();
		 listToEmit.add("x9/QBJZJLHHXJhSze9s12Hc82ds=");
         listToEmit.add(JsonUtils.createJsonFromStringStringMap(varaibleValueMap));
         listToEmit.add("Telluride");
        listToEmit.add("7081285724292671");
        
        this.outputCollector.emit(listToEmit);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source","lyl_id_no"));
		
	}
}
