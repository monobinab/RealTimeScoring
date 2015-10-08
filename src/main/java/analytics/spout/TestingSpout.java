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
		varaibleValueMap.put("BLACKOUT_MATTRESS", "0.0");
		/*varaibleValueMap.put("BLACKOUT_WASH_DRY", "0.0");
		varaibleValueMap.put("BLACKOUT_REGRIG", "0.0");*/
	//	varaibleValueMap.put("BLACKOUT_WASH_DRY", "0.0");
		
		List<Object> listToEmit = new ArrayList<Object>();
		 listToEmit.add("+gG4eW5Pt3rfD+C//YCeGvX5cfQ=");
         listToEmit.add(JsonUtils.createJsonFromStringStringMap(varaibleValueMap));
         listToEmit.add("Telluride");
        listToEmit.add("7081237551308377");
        
        this.outputCollector.emit(listToEmit);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source","lyl_id_no"));
		
	}
}
