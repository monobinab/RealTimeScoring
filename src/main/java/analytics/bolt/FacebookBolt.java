package analytics.bolt;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import analytics.util.dao.FBLoyaltyIdDao;
import analytics.util.dao.FbVariableDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class FacebookBolt extends BaseRichBolt{

	private OutputCollector collector;
    
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {		
		String message = (String)input.getValueByField("message");
		String[] messageArray = message.split(",");
		String timestamp = messageArray[0];
		String id = messageArray[1];
		String positive = messageArray[2];
		//TODO: We currently ignore the negative score, else it is messageArray[3]
		String topic = messageArray[4];
		
		//TODO: Uncomment below fake id to get things running propertly. 
		//Currently we do not have enough fb - loyalty mapping to run things without the hard coding
		id = "1298910293";
		String str = new FBLoyaltyIdDao().getLoyaltyIdFromID(id);
		if (str!=null) {    
		    //Find the list of variables that are affected by the model mentioned - We need the FB string to variable mapping
		    
		    //How is model name to variable mapping?? - BOOST vars are added to the respective models
		    Gson gson = new Gson();
		    Map<String,String> variableValueMap = new HashMap<String, String>();
		    //TODO: Maybe they also map to multiple variables
		    variableValueMap.put(new FbVariableDao().getVariableFromTopic(topic), positive.substring(1, positive.indexOf(']')));
		    Type varValueType = new TypeToken<Map<String, String>>() {}.getType();
	    	String varValueString = gson.toJson(variableValueMap, varValueType);
        	List<Object> listToEmit = new ArrayList<Object>();
        	listToEmit.add(str);
        	listToEmit.add(varValueString);
        	listToEmit.add("FB");
        	System.out.println(" @@@ FB PARSING BOLT EMITTING: " + listToEmit);
        	this.collector.emit(listToEmit);
        		    
		}
		//Create a boost. new variable in model variables colelction 
		//BOOST_*
		//Insert the variable into Variables
		}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		// no output fields. We just insert the record to mongo and we are done??
		//TODO: How to implement the rest of it. Trigger- change score..etc
		declarer.declare(new Fields("l_id","fbScores","source"));
	}

}
