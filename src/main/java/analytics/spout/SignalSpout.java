package analytics.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.JSONArray;
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
	private SpoutOutputCollector collector;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			
			JSONArray feedJsonArray = new JSONArray( HttpClientUtils.httpGetCallJsonString(Constants.SIGNAL_URL));
			//System.out.println("incoming json " + feedJsonArray);
			for(int i=0; i<feedJsonArray.length();i++){
				try{
				List<Object> listToEmit = new ArrayList<Object>();
				JSONObject jsonObj = (JSONObject) feedJsonArray.get(i);
			//	LOGGER.info(jsonObj.toString());
				String valueString = (String) jsonObj.get("value");
				JSONObject valueJsonObj = new JSONObject(valueString);
				/*
				 * need to be removed, logging needed for testing
				 */
				if(valueJsonObj.get("type").toString().equalsIgnoreCase("BrowseProduct")){
					LOGGER.info("PERSIST: ProductBrowse signal " + jsonObj.toString());
				}
				JSONObject userJsonObj = (JSONObject) valueJsonObj.get("user");
				listToEmit.add(valueJsonObj.get("channel"));
				if(valueJsonObj.has("products"))
					listToEmit.add(valueJsonObj.get("products"));
				else
					listToEmit.add(null);
				if(valueJsonObj.has("searchTerm"))
					listToEmit.add(null);
				else
					listToEmit.add(null);
				listToEmit.add(valueJsonObj.get("signalTime"));
				listToEmit.add(valueJsonObj.get("source"));
				if(valueJsonObj.has("taxonomy"))
					listToEmit.add(valueJsonObj.get("taxonomy"));
				else
					listToEmit.add(null);
				if(userJsonObj.has("uuid"))
					listToEmit.add(userJsonObj.get("uuid"));
				else
					listToEmit.add(null);
				listToEmit.add(valueJsonObj.get("type"));
				listToEmit.add(jsonObj.get("offset"));
				collector.emit(listToEmit);
				//nullifying the objects once emitted successfully 
				listToEmit = null;
				valueJsonObj = null;
				}
				catch(Exception e2){
					e2.printStackTrace();
					LOGGER.error("Exception in json ", ExceptionUtils.getFullStackTrace(e2) +": " + ExceptionUtils.getRootCauseMessage(e2) + ": " + ExceptionUtils.getMessage(e2));
				}
			}
			feedJsonArray = null;
	} catch (Exception e) {
			LOGGER.error("Exception in SignalSpout " , ExceptionUtils.getMessage(e) );
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("channel", "products", "searchTerm", "signalTime", "source", "taxonomy", "uuid", "type", "offset"));
	}

}