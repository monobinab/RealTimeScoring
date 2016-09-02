package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import analytics.util.BrowseUtils;
import analytics.util.DCParsingHandler;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.dao.DcAidVarStrengthDao;
import analytics.util.objects.DC;
import analytics.util.objects.ParsedDC;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsingBoltDC extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(ParsingBoltDC.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	private DcAidVarStrengthDao dcAidVarStrengthDao;
	private BrowseUtils browseUtils;
	private String browseKafkaTopic;
	private String source;
	
	 public ParsingBoltDC(String systemProperty, String browseKafkaTopic, String source){
		 super(systemProperty);
		 this.browseKafkaTopic = browseKafkaTopic;
		 this.source = source;
	 }
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		dcAidVarStrengthDao = new DcAidVarStrengthDao();
		browseUtils = new BrowseUtils(System.getProperty(MongoNameConstants.IS_PROD), browseKafkaTopic);
		LOGGER.info("DC Bolt Preparing to Launch");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields("l_id", "varValueMapAsJsonString", "source", "lyl_id_no"));
	}
	 
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		JSONObject obj = null;
		ParsedDC parsedDC = null;
		try {
			if(input.contains("str")){
				String message = (String) input.getValueByField("str");
				
				//check the incoming string for <UpdateMemberPrompts or :UpdateMemberPrompts as it contains the member's response data
				if (message.contains("<UpdateMemberPrompts") || message.contains(":UpdateMemberPrompts")) {
					redisCountIncr("prompts_reply");
					
						obj = new JSONObject(message);
						
						//xmlReqData contains the answerChoiceIds which is needed
						message = (String) obj.get("xmlReqData");
						
						LOGGER.info("xmlReqData: " + message);
														
						//ParsedDC parses the xml and return the list of answerIds along with memberNumber
						parsedDC = DCParsingHandler.getAnswerJson(message);
						if(parsedDC != null){
							processAidsList(parsedDC);
						}
						else{
							outputCollector.ack(input);
							return;
						}
				}
				else{
					outputCollector.ack(input);
					return;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("exception in parsingBoltDC ", e);
		}
		/*
		 * nullifying the objects
		 */
		obj = null;
		parsedDC = null;
		outputCollector.ack(input);
	}
	
	protected void processAidsList(ParsedDC parsedDC) throws JSONException {
		
	/*   1. In this processing of parsedDC object, answerIds are iterated 
		 2. variables for those answerIds are retrieved from dcVariableStrength collection
		 3. variableValueMap is populated with variables as keys and strength as values for it
		 4. For answerChoiceIds, which has tags associated with them, tags are populated in a set and got publihsed to kafka*/
		String loyalty_id = parsedDC.getMemberId();
		String l_id = SecurityUtils.hashLoyaltyId(loyalty_id);
		Double strength_sum = 0.0;
		Map<String, String> variableValueMap = new HashMap<String, String>();
		Map<String, DC> dcMap = dcAidVarStrengthDao.getdcAidVarStrenghtTags();
		List<String> answerChoiceIds = parsedDC.getAnswerChoiceIds();
		if(answerChoiceIds != null && !answerChoiceIds.isEmpty()){
		Iterator<String> answerChoiceIdsIterator =  answerChoiceIds.iterator();
		Set<String> dcTagsSet = new HashSet<String>();
		while(answerChoiceIdsIterator.hasNext()){
			String aid = answerChoiceIdsIterator.next();
			DC dcDto =null;
			if(dcMap.containsKey(aid)){
				dcDto = dcMap.get(aid);
				Map<String, Integer> varStrengthMap = dcDto.getBoostStrengthMap();
				if(varStrengthMap != null){
					for(String var : varStrengthMap.keySet()){
						if(!variableValueMap.containsKey(var)){
							variableValueMap.put(var, Double.toString(varStrengthMap.get(var)));
						}
						else{
							strength_sum = Double.parseDouble(variableValueMap.get(var)) + varStrengthMap.get(var);
							variableValueMap.put(var,  Double.toString(strength_sum));
						}
					}
				}
			}
			else{
				continue;
			}
			
			if(dcDto.getTags() != null && !dcDto.getTags().isEmpty()){
				dcTagsSet.addAll(dcDto.getTags());
			}
		}
			if(variableValueMap != null && !variableValueMap.isEmpty()){
					emitToScoreStream(l_id, variableValueMap, loyalty_id);
			}
			else{
				LOGGER.info("varValueMap is null or empty for " + l_id);
			}
			publishToKafka(dcTagsSet, loyalty_id, l_id);
	}
}
	
	public void publishToKafka(Set<String> buSubBuListToKafka, String loyalty_id, String l_id){
		//publish to kafka
		if(buSubBuListToKafka != null && !buSubBuListToKafka.isEmpty()){
			browseUtils.publishToKafka(buSubBuListToKafka, loyalty_id, l_id, source, browseKafkaTopic);
			LOGGER.info("PERSIST: DC tags " + buSubBuListToKafka + " got published to kafka for " + l_id);
			redisCountIncr("dcTag_to_kafka");
		}
	}
	public void emitToScoreStream(String l_id, Map<String, String> varValueMap, String loyalty_id){
		List<Object> listToEmit_s = new ArrayList<Object>();
		listToEmit_s.add(l_id);
		listToEmit_s.add(JsonUtils.createJsonFromStringStringMap(varValueMap));
		listToEmit_s.add("DC");
		listToEmit_s.add(loyalty_id);
		outputCollector.emit(listToEmit_s);
		redisCountIncr("emitted_to_scoring");
		LOGGER.info("Emitted message to scoring for l_id from DC " + l_id);
		//System.out.println("Emitted message to score stream for l_id from DC " + l_id);
	}
}
