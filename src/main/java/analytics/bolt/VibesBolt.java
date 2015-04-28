package analytics.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.HttpClientUtils;
import analytics.util.dao.VibesDao;
import analytics.util.objects.Vibes;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class VibesBolt extends EnvironmentBolt{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(VibesBolt.class);
	private VibesDao vibesDao;
	private OutputCollector outputCollector;
	
	 public VibesBolt(String systemProperty){
		 super(systemProperty);
	 }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		//vibesDao = new VibesDao();
	}
	
	@Override
	public void execute(Tuple input) {
		LOGGER.info("~~~~~~~~~~Incoming tuple in Vibesbolt: " + input);
		
		redisCountIncr("incoming_tuples");
		Vibes vibes = (Vibes) input.getValueByField("vibesDBObject");
		String lyl_id_no = null;
		String event_type = null;

		try {
			lyl_id_no = (String) vibes.getLyl_id_no();
			event_type = (String) vibes.getEvent_type();
			LOGGER.info("PROCESSING L_Id: " + lyl_id_no +" for Event Type " + event_type);
			
			JSONObject vibesJson = generateJson(lyl_id_no,event_type);
			
			sendMessageToVibes(vibesJson.toString());
			
			//vibesDao.updateVibes(l_id);
			
			redisCountIncr("success_vibes");
			outputCollector.ack(input);
		} catch (Exception e) {
			LOGGER.error("Exception Occured at Vibes Bolt for Lid" + lyl_id_no );
			e.printStackTrace();
			
			redisCountIncr("failure_vibes");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	public void sendMessageToVibes(String jsonString) throws IOException{
		
		BufferedReader in = null;
		OutputStreamWriter out = null;
		StringBuffer strBuff = new StringBuffer();
		HttpURLConnection connection = null;
		
		try{
			connection = HttpClientUtils.getConnectionWithBasicAuthentication(AuthPropertiesReader
					.getProperty(Constants.VIBES_URL),"application/json", "POST",AuthPropertiesReader
					.getProperty(Constants.VIBES_USER_NAME), AuthPropertiesReader
					.getProperty(Constants.VIBES_PASSWORD));
			
			out = new OutputStreamWriter(connection.getOutputStream());
			LOGGER.debug("After Creating outWriter");
	
			out.write(jsonString);
			out.close();
	
			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			int c;
			while ((c = in.read()) != -1) {
				strBuff.append((char) c); 
			}
			
			System.out.println("Vibes Response ==> " + strBuff.toString());
			
		}catch (Exception t) {
			t.printStackTrace();
			LOGGER.error("Exception occured in sendMessageToVibes for Lid+EventType = " + jsonString);
		} finally {
			try {
				if(out!=null) 
					out.close(); 
				if (in != null) 
					in.close();
				if (connection != null) 
					connection.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.error("Exception occured in sendMessageToVibes: finally: catch block ", e);
			}
		}
		
	}
	
	public JSONObject generateJson(String lyl_id_no, String event_type){
		
		JSONObject obj = new JSONObject();
		
		try {
			obj.put("event_id", lyl_id_no+"~"+event_type);
			obj.put("event_type", event_type);
			obj.put("event_date", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
			
			JSONObject memberObj = new JSONObject();
			memberObj.put("external_person_id", lyl_id_no);
			
			obj.put("event_data", memberObj);
			
		} catch (JSONException e) {
			e.printStackTrace();
			LOGGER.info("Exception in generateJson in Vibes Bolt for Lid+EventType = " + lyl_id_no+"~"+event_type);
		}
		
		return obj;
	}

}
