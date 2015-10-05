package analytics.bolt;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.SecurityUtils;
import analytics.util.TupleParser;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.ModelScore;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

public class PurchaseBolt extends EnvironmentBolt  {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(PurchaseBolt.class);
	private OutputCollector outputCollector;
	
	public PurchaseBolt(String env) {
		super(env);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;			
	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("PurchaseBolt_input_count");
		
		countMetric.scope("entering_Purchase_bolt").incr();			
			
		if(input != null)
		{
				try{
				JsonElement jsonElement = TupleParser.getParsedJson(input);
				LOGGER.info("PERSIST: Input from PurchaseSpout :" + jsonElement.toString());
				JsonElement lyl_id_no = jsonElement.getAsJsonObject().get("memberId");
				
				if (lyl_id_no == null) {
					LOGGER.error("Invalid incoming json with empty loyalty id");
					outputCollector.ack(input);
					redisCountIncr("invalid_loy_id_count");
					return;
				}
				if (lyl_id_no.getAsString().length() != 16) {
					LOGGER.error("PERSIST: invalid loyalty id -" +lyl_id_no.getAsString());
					outputCollector.ack(input);
					redisCountIncr("invalid_loy_id_count");
					return;
				}
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no.getAsString());
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit = new ArrayList<Object>();
				listToEmit.add(lyl_id_no.getAsString());
				listToEmit.add(l_id);				
				this.outputCollector.emit(listToEmit);
				
				
			} catch (Exception e){
				LOGGER.error("PERSIST: Exception Occured in PurchaseBolt :: " +  e.getMessage()+ "  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
				redisCountIncr("exception_count");
			}
				
		} else {
			redisCountIncr("null_lid");					
		}
		outputCollector.ack(input);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lyl_id_no", "l_id"));
	}
	
}