package analytics.spout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
	BufferedWriter bw;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		
		
		//needs to be removed
		
		try{
		File file = new File("C://users/kmuthuk/Desktop/signaloutput2.txt");
		 
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		 bw = new BufferedWriter(fw);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		try {
			
			JSONArray feedJsonArray = new JSONArray( HttpClientUtils.httpGetCallJsonString(Constants.SIGNAL_URL));
			//System.out.println("incoming json " + feedJsonArray);
			for(int i=0; i<feedJsonArray.length();i++){
				List<Object> listToEmit = new ArrayList<Object>();
				JSONObject jsonObj = (JSONObject) feedJsonArray.get(i);
				String valueString = (String) jsonObj.get("value");
				JSONObject valueJsonObj = new JSONObject(valueString);
				JSONObject userJsonObj = (JSONObject) valueJsonObj.get("user");
				String out = valueJsonObj.get("signalTime") + " " + valueJsonObj.get("source") + " " + jsonObj.get("offset");
				bw.write(out);
				bw.newLine(); 
				/*listToEmit.add(valueJsonObj.get("channel"));
				listToEmit.add(valueJsonObj.get("products"));
				listToEmit.add(valueJsonObj.get("searchTerm"));
				listToEmit.add(valueJsonObj.get("signalTime"));
				listToEmit.add(valueJsonObj.get("source"));
				listToEmit.add(valueJsonObj.get("taxonomy"));
				listToEmit.add(userJsonObj.get("uuid"));
				listToEmit.add(valueJsonObj.get("type"));
				listToEmit.add(jsonObj.get("offset"));
				collector.emit(listToEmit);*/
				//nullifying the objects once emitted successfully 
				listToEmit = null;
				valueJsonObj = null;
			}
			feedJsonArray = null;
			//Thread.sleep(30000); // has to sleep for 30secs
		} catch (Exception e) {
			LOGGER.error("Exception in SignalSpout " , e.getClass() + ": " + e.getMessage());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	//	declarer.declare(new Fields("channel", "products", "searchTerm", "signalTime", "source", "taxonomy", "uuid", "type", "offset"));
	}

}