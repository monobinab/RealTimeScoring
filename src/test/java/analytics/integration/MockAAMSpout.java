package analytics.integration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.mockito.Mockito;

import com.ibm.jms.JMSMessage;

import analytics.util.JsonUtils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MockAAMSpout extends BaseRichSpout{
	SpoutOutputCollector collector;
	static List<Object> testData;
	//Tuple tuple = mock(Tuple.class);
	/*
	 * 
	 */
    
	public static void mockTuple() {
		String l_id="Ay3oe4p4bir7S6Le1ailwe/4n5A=";
    	String message =  "{\"M_WEB_TRAIT_POWER_TOOL_8_14\":\"{\\\"2014-10-06\\\":[\\\"83614\\\",\\\"81194\\\",\\\"80630\\\"]}\""
    			//+ ",\"M_WEB_TRAIT_MOWER_15_30\":\"{\\\"2014-10-06\\\":[\\\"83614\\\",\\\"81194\\\",\\\"80630\\\"]}\""
    			//+ ",\"M_WEB_DAY_SNOW_BLOWER_0_7\":\"{\\\"2014-10-06\\\":[\\\"83614\\\",\\\"81194\\\",\\\"80630\\\"]}\""
    			//+ ",\"M_WEB_TRAIT_POWER_TOOL_0_7\":\"{\\\"2014-10-06\\\":[\\\"83614\\\",\\\"81194\\\",\\\"80630\\\"]}\""
    			//+ ",\"M_WEB_TRAIT_TRACTOR_15_30\":\"{\\\"2014-10-06\\\":[\\\"83614\\\",\\\"81194\",\\\"80630\\\"]}\""
    			//+ ",\"M_WEB_TRAIT_MOWER_0_7\":\"{\\\"2014-10-06\\\":[\\\"83614\\\",\\\"81194\\\",\\\"80630\\\"]}\""
    			+ "}";
    	String source = "WebTraits";
        /*Tuple tuple = mock(Tuple.class);
    
        
        when(tuple.getStringByField("l_id")).thenReturn(l_id);
        when(tuple.getStringByField("source")).thenReturn(source);
        when(tuple.getStringByField("message")).thenReturn(message);
        when(tuple.getValueByField("message")).thenReturn(message);	  
        when(tuple.getString(1)).thenReturn(message);
        when(tuple.getValues()).thenReturn(new Values(message));
        
        when(tuple.getValueByField("source")).thenReturn(source);
        return tuple;*/
    	testData = new ArrayList<Object>();
    	testData.add(l_id);
    	testData.add(message);
    	testData.add(source);


    } 

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		System.out.println("~~~~~~~~~~~~~~~~TESTING THE STORMCONF IN INTEGRATION TESTING OF AAMTRAITS~~~~~~~~~~~~~~~" + conf.get("nimbus.host"));
		this.collector = collector;
    	mockTuple();
	}

	@Override
	public void nextTuple() {
		if(testData!=null && !testData.isEmpty()){
			collector.emit(testData);
			testData = null;
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id","lineItemAsJsonString","source"));
		
	}

}
