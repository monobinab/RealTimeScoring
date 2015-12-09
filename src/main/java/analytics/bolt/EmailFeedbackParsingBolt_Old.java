package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.EmailBuVariablesDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.objects.VariableModel;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class EmailFeedbackParsingBolt_Old extends EnvironmentBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(EmailFeedbackParsingBolt_Old.class);
	private OutputCollector outputCollector;
	//private Map<String, List<String>> emailBlackoutVariablesMap;
	//private EmailBuSubBuBlackoutVariablesDao emailBuSubBuBlackoutVariablesDao;
	private String topologyName;
	
	//Sree Changes for Email Feedback = Y
	private EmailBuVariablesDao emailBuVariablesDao;
	private Map<String, List<VariableModel>> emailBUVariablesMap;
	private ModelPercentileDao modelPercentileDao;
	
	public EmailFeedbackParsingBolt_Old(String env) {		
		super(env);			
	}	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		super.prepare(stormConf, context, collector);		
		this.outputCollector = collector;	
		//emailBuSubBuBlackoutVariablesDao = new EmailBuSubBuBlackoutVariablesDao();
		topologyName = (String) stormConf.get("metrics_topology");
		
		emailBuVariablesDao = new EmailBuVariablesDao();
		emailBUVariablesMap = emailBuVariablesDao.getEmailBUVariables();
		modelPercentileDao = new ModelPercentileDao();
	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		JsonParser parser = new JsonParser();
		JsonElement emailFeedbackObject = parser.parse(input.getValueByField("str").toString());
		String lyl_id_no = emailFeedbackObject.getAsJsonObject().get("lyl_id_no").getAsString();
		String bu = emailFeedbackObject.getAsJsonObject().get("BU").getAsString();//HA, AA
		String format = emailFeedbackObject.getAsJsonObject().get("format").getAsString();//S/K
		String emailFeedback = emailFeedbackObject.getAsJsonObject().get("emailFeedbackResponse").getAsString();//YES/NO	
		
		LOGGER.info("Incoming Message to EmailFeedbackParsingBolt: " + input.toString());
			
		if (lyl_id_no == null) {
			redisCountIncr("null_lid");
			outputCollector.ack(input);
			return;
		} else {		
			// RTS only wants encrypted loyalty ids
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
			
			Map<String, String> variableValueMap = new HashMap<String, String>();
			
			if(emailFeedback.equals("NO")){//blackout 
				/*emailBlackoutVariablesMap = emailBuSubBuBlackoutVariablesDao.getEmailBlackoutVariables(bu,format);
				if(emailBlackoutVariablesMap.containsKey(bu)){
					for (String b : emailBlackoutVariablesMap.get(bu)) {					
							variableValueMap.put(b, "1");	//any value could have been suffice as Blackout strategy will reset the core to 0 anyway			
					}	
				}*/
				
				String key = bu+"~"+format;
				if(emailBUVariablesMap.get(key)!=null && emailBUVariablesMap.get(key).size()>0){
					System.out.println("NO - Email list for " + key + " is "+emailBUVariablesMap.get(key));
					for (VariableModel var : emailBUVariablesMap.get(key)) {
						if (var.getVariable().toUpperCase().contains("BLACK"))
							variableValueMap.put(var.getVariable(), "1");			
					}
				}
				
				redisCountIncr("email_feedback_no_count");
			}
			else if(emailFeedback.equals("YES")){//boost
				//Sree. get email boost variables
				String key = bu+"~"+format;
				if(emailBUVariablesMap.get(key)!=null && emailBUVariablesMap.get(key).size()>0){
					System.out.println("YES - Email list for " + key + " is "+emailBUVariablesMap.get(key));
					for (VariableModel var : emailBUVariablesMap.get(key)) {									
						variableValueMap.put(var.getVariable(), isBlackOutVariable(var.getVariable()) ? "0" : 
							getVariableValue(var.getModelId()));			
					}
				}
				redisCountIncr("email_feedback_yes_count");
			}
			
			if (variableValueMap != null && !variableValueMap.isEmpty()) {
				
				String varValueJsonString = (String) JsonUtils.createJsonFromStringStringMap(variableValueMap);
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit.add(l_id);
				listToEmit.add(varValueJsonString);
				listToEmit.add(topologyName);
				listToEmit.add(lyl_id_no);
				
				if (listToEmit != null && !listToEmit.isEmpty()) {				
					LOGGER.debug(" @@@ Emit from Email Feedback Parsing bolt: " + varValueJsonString + " for lid: " 
										+ lyl_id_no + " at "+ System.currentTimeMillis());
					this.outputCollector.emit("score_stream", listToEmit);				
					redisCountIncr("successful");
				}

			} else {
				redisCountIncr("empty_var_map");
			}
			this.outputCollector.ack(input);

		}
	}
	
	private String getVariableValue(int modelId){
		
		return modelPercentileDao.getSingleModelPercentile(modelId+"", "95");
	}
	
	private Boolean isBlackOutVariable(String variable){
		
		if(variable.toUpperCase().contains("BLACK"))
			return true;
		
		return false;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source", "lyl_id_no"));
		declarer.declareStream("score_stream",new Fields("l_id", "lineItemAsJsonString", "source", "lyl_id_no"));
	}

}
