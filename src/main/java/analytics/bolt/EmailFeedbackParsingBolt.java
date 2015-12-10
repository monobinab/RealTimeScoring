package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.BrowseUtils;
import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.BoostBrowseBuSubBuDao;
import analytics.util.dao.CpsOccasionsDao;
import analytics.util.dao.EmailBuVariablesDao;
import analytics.util.dao.ModelPercentileDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.objects.BoostBrowseBuSubBu;
import analytics.util.objects.VariableModel;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class EmailFeedbackParsingBolt extends EnvironmentBolt {
	
	private static final String BOOST_VALUE = "4";
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(EmailFeedbackParsingBolt.class);
	private OutputCollector outputCollector;
	private String topologyName;
	
	
	//Sree Changes for Email Feedback = Y
	private EmailBuVariablesDao emailBuVariablesDao;
	private Map<String, List<VariableModel>> emailBUVariablesMap;
	private ModelPercentileDao modelPercentileDao;
	
	/*
	 * for browse tag creation
	*/
	private TagVariableDao tagVariableDao;
	private Map<Integer, String> tagModelIdMap;
	private BoostBrowseBuSubBuDao boostBrowseBuSubBuDao;
	private Map<String, BoostBrowseBuSubBu> boostBrowseBuSubBuMap;
	private BrowseUtils browseUtils;
	private String systemProperty;
	private String browseKafkaTopic;
	private String web;
	private Map<String, String> occasionIdMap;
	private CpsOccasionsDao cpsOccasionsDao;
	
	public EmailFeedbackParsingBolt(String env, String web, String browseKafkaTopic) {		
		super(env);	
		this.systemProperty = env;
		this.browseKafkaTopic = browseKafkaTopic;
		this.web = web;
	}	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		super.prepare(stormConf, context, collector);		
		this.outputCollector = collector;	
		topologyName = (String) stormConf.get("metrics_topology");
		browseUtils = new BrowseUtils(systemProperty, browseKafkaTopic);
		emailBuVariablesDao = new EmailBuVariablesDao();
		emailBUVariablesMap = emailBuVariablesDao.getEmailBUVariables();
		modelPercentileDao = new ModelPercentileDao();
		tagVariableDao = new TagVariableDao();
		tagModelIdMap = tagVariableDao.getTagFromModelId();
		boostBrowseBuSubBuDao = new BoostBrowseBuSubBuDao();
		boostBrowseBuSubBuMap = boostBrowseBuSubBuDao.getBoostBuSubBuFromModelCode();
		cpsOccasionsDao = new CpsOccasionsDao();
		occasionIdMap = cpsOccasionsDao.getcpsOccasionId();
	}
	
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		
	try{
		JsonParser parser = new JsonParser();
		JsonElement emailFeedbackObject = parser.parse(input.getValueByField("str").toString());
		String lyl_id_no = emailFeedbackObject.getAsJsonObject().get("lyl_id_no").getAsString();
		String bu = emailFeedbackObject.getAsJsonObject().get("BU").getAsString();//HA, AA
		String format = emailFeedbackObject.getAsJsonObject().get("format").getAsString();//S/K
		String emailFeedback = emailFeedbackObject.getAsJsonObject().get("emailFeedbackResponse").getAsString();//YES/NO	
		String key = bu+"~"+format;
		LOGGER.info("Incoming Message to EmailFeedbackParsingBolt: " + input.toString());
			
		if (lyl_id_no == null) {
			redisCountIncr("null_lid");
			outputCollector.ack(input);
			return;
		} else {		
			// RTS only wants encrypted loyalty ids
			String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
			
			Map<String, String> variableValueMap = new HashMap<String, String>();
			
			//buSubBuList to be sent to kafka for BrowseTag creation
			Set<String> buSubBuListtoKafka = new HashSet<String>();
			
			if(emailFeedback.equals("NO")){//blackout 
			
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
				if(emailBUVariablesMap.get(key)!=null && emailBUVariablesMap.get(key).size()>0){
					System.out.println("YES - Email list for " + key + " is "+emailBUVariablesMap.get(key));
					for (VariableModel var : emailBUVariablesMap.get(key)) {									
						variableValueMap.put(var.getVariable(), isBlackOutVariable(var.getVariable()) ? "0" : 
							getVariableValue(var.getModelId()));			
					}
				}
				redisCountIncr("email_feedback_yes_count");
			}
		
			else if(emailFeedback.equals("EXPLICIT")){
				if(emailBUVariablesMap.get(key)!=null && emailBUVariablesMap.get(key).size()>0){
					for (VariableModel var : emailBUVariablesMap.get(key)) {
						LOGGER.info("EXPLICIT - Email list for " + key +"is " + emailBUVariablesMap.get(key) + " lid " + l_id);
						/*int modelId = var.getModelId();
						String tag  = tagModelIdMap.get(modelId);
						String boostBrowseVar = boostBrowseBuSubBuMap.get(tag).getBoost();*/
						
						
						/*
						 * modelId is checked whether it is tagVar collection to get the tag from it
						 * the retrieved tag is checked for its existence in boostBrowseBuSubBu collection to get the BOOST_BROWSE variable
						 * for scoring and to get subBus for browse tag creation
						 */
						if(tagModelIdMap.containsKey(var.getModelId()) && boostBrowseBuSubBuMap.containsKey(tagModelIdMap.get(var.getModelId()))){
							//For scoring
							variableValueMap.put(boostBrowseBuSubBuMap.get(tagModelIdMap.get(var.getModelId())).getBoost(), BOOST_VALUE);
						
							//For BrowseTag creation
							if(boostBrowseBuSubBuMap.get(tagModelIdMap.get(var.getModelId())).getBuSubBu() != null)
								buSubBuListtoKafka.add(boostBrowseBuSubBuMap.get(tagModelIdMap.get(var.getModelId())).getBuSubBu()+occasionIdMap.get(web));
							
						}
					}
				}
					redisCountIncr("email_feedback_explicit_count");
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
			
			//publishing to kafka for browseTag creation
			if(buSubBuListtoKafka != null && !buSubBuListtoKafka.isEmpty()){
				browseUtils.publishToKafka(buSubBuListtoKafka, lyl_id_no, l_id, topologyName, browseKafkaTopic);
				redisCountIncr("explicit_to_kafka");
			}
			else{
				redisCountIncr("explicit_not_to_kafka");
			}
		}
	}
	catch(Exception e){
		LOGGER.error("Exception in EmailFeedbackParsng bolt "  + e);
	}
		this.outputCollector.ack(input);
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
