package analytics.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.HttpClientUtils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class SignalSpout extends BaseRichSpout{
	private static final Logger LOGGER = LoggerFactory.getLogger(SignalSpout.class);
	String signalUrl = null;
    private SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		signalUrl = Constants.SIGNAL_URL;
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			
			JSONArray feedJsonArray = new JSONArray( HttpClientUtils.httpGetCallJsonString(signalUrl));
			
			for(int i=0; i<feedJsonArray.length();i++){
				List<Object> listToEmit = new ArrayList<Object>();
				JSONObject jsonObj = (JSONObject) feedJsonArray.get(i);
				String valueString = (String) jsonObj.get("value");
				JSONObject valueJsonObj = new JSONObject(valueString);
				JSONObject userJsonObj = (JSONObject) valueJsonObj.get("user");
				listToEmit.add(valueJsonObj.get("channel"));
				listToEmit.add(valueJsonObj.get("products"));
				listToEmit.add(valueJsonObj.get("searchTerm"));
				listToEmit.add(valueJsonObj.get("signalTime"));
				listToEmit.add(valueJsonObj.get("source"));
				listToEmit.add(valueJsonObj.get("taxonomy"));
				listToEmit.add(userJsonObj.get("uuid"));
				listToEmit.add(valueJsonObj.get("type"));
				collector.emit(listToEmit);
			}
			
		} catch (JSONException e) {
			LOGGER.error("Exception in SignalSpout " , e.getClass() + " " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("channel", "products", "searchTerm", "signalTime", "source", "taxonomy", "uuid", "type"));
	}

}