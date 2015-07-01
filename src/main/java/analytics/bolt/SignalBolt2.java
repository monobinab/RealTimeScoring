package analytics.bolt;

import static backtype.storm.utils.Utils.tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.HttpClientUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.VibesDao;
import analytics.util.objects.RtsCommonObj;
import analytics.util.objects.Vibes;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SignalBolt2 extends EnvironmentBolt{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SignalBolt2.class);
	private OutputCollector outputCollector;
	
	 public SignalBolt2(String systemProperty){
		 super(systemProperty);
	 }

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		LOGGER.info("~~~~~~~~~~Incoming tuple in Vibesbolt: " + input);
		
		redisCountIncr("incoming_tuples");
		RtsCommonObj rtsCommonObj = (RtsCommonObj) input.getValueByField("rtsCommonObj");
		String lyl_id_no = null;
		ArrayList<String> pidLst = null;

		try {
				lyl_id_no = rtsCommonObj.getLyl_id_no();
				pidLst = rtsCommonObj.getPidList();
				LOGGER.info("PROCESSING L_Id: " + lyl_id_no +" with PIDs " + pidLst);
				
				
				if(lyl_id_no != null && pidLst != null && pidLst.size()>0){
					//String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
					
					String pidLstStr = StringUtils.join(pidLst, ',');
					String str = "[,"+lyl_id_no+","+pidLstStr+",]";
					
					outputCollector.emit(tuple(str));
					
					str = null;
					pidLstStr = null;
					
				}else
					LOGGER.info("Either L_Id is null or Pid List is null. No sending to Parsing Bolt .. Input Tuple : " +input);
			
			redisCountIncr("success_signal2");
			outputCollector.ack(input);
		} catch (Exception e) {
			LOGGER.error("Exception Occured at Signal Bolt 2 for Lid" + lyl_id_no );
			e.printStackTrace();
			
			redisCountIncr("failure_signal2");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
