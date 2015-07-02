package analytics.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import analytics.util.DCParsingHandler;
import analytics.util.JsonUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.DcAidVarStrengthDao;
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
	private Map<String, Map<String, Double>> dcAidVarStrengthMap;

	 public ParsingBoltDC(String systemProperty){
		 super(systemProperty);
	 }
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		dcAidVarStrengthDao = new DcAidVarStrengthDao();
		dcAidVarStrengthMap = dcAidVarStrengthDao.getdcAidVarStrenghtMap();
		//System.out.println(dcAidVarStrengthMap.size());
		LOGGER.info("DC Bolt Preparing to Launch");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields("l_id", "varValueMapAsJsonString", "source"));
	}
	 
	@Override
	public void execute(Tuple input) {
		redisCountIncr("incoming_tuples");
		String message = (String) input.getValueByField("str");
		
		//check the incoming string for <UpdateMemberPrompts or :UpdateMemberPrompts as it contains the member's response data
		if (message.contains("<UpdateMemberPrompts") || message.contains(":UpdateMemberPrompts")) {
			redisCountIncr("prompts_reply");
			try {
				JSONObject obj = new JSONObject(message);
				
				//xmlReqData contains the answerChoiceIds which is needed
				message = (String) obj.get("xmlReqData");
				
				LOGGER.info("xmlReqData: " + message);
							
				//ParsedDC parses the xml and return the list of answerIds along with memberNumber
				ParsedDC parsedDC = DCParsingHandler.getAnswerJson(message);
				if(parsedDC != null){
					processAidsList(parsedDC);
				}
				else{
					outputCollector.ack(input);
					return;
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error("exception in parsingBoltDC ", e);
			}
		}
		else{
			outputCollector.ack(input);
			return;
		}
		outputCollector.ack(input);
	}
	
	protected void processAidsList(ParsedDC parsedDC) throws JSONException {
		
	/*   1. In this processing of parsedDC object, answerIds are iterated 
		 2. variables for those answerIds are retrieved from dcVariableStrength collection
		 3. variableValueMap is populated with variables as keys and strength as values for it*/
		 
		String l_id = SecurityUtils.hashLoyaltyId(parsedDC.getMemberId());
		Double strength_sum = 0.0;
		Map<String, String> variableValueMap = new HashMap<String, String>();
		List<String> answerChoiceIds = parsedDC.getAnswerChoiceIds();
		if(answerChoiceIds != null && !answerChoiceIds.isEmpty()){
		Iterator<String> answerChoiceIdsIterator =  answerChoiceIds.iterator();
		while(answerChoiceIdsIterator.hasNext()){
			String aid = answerChoiceIdsIterator.next();
			Map<String, Double> varStrengthMap = dcAidVarStrengthMap.get(aid);
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
			else{
				continue;
			}
		}
			emitToScoreStream(l_id, variableValueMap);
	}
}	
	public void emitToScoreStream(String l_id, Map<String, String> varValueMap){
		List<Object> listToEmit_s = new ArrayList<Object>();
		listToEmit_s.add(l_id);
		listToEmit_s.add(JsonUtils.createJsonFromStringStringMap(varValueMap));
		listToEmit_s.add("DC");
		outputCollector.emit(listToEmit_s);
		redisCountIncr("emitted_to_scoring");
		LOGGER.info("Emitted message to scoring for l_id from DC " + l_id);
		//System.out.println("Emitted message to score stream for l_id from DC " + l_id);
	}
}
