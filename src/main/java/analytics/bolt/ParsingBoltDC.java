package analytics.bolt;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;

import analytics.util.Constants;
import analytics.util.DCParserHandler;
import analytics.util.HostPortUtility;
import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.dao.DCDao;
import analytics.util.dao.MemberDCDao;
import analytics.util.objects.TransactionLineItem;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mongodb.DB;

public class ParsingBoltDC extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(ParsingBoltDC.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	private DCDao dc;
	private MemberDCDao memberDCDao;
	private Type varValueType;
	private MultiCountMetric countMetric;
	//needs to be removed after development is completed and moved to prod
	private static final boolean isTestL_ID = false;
	private static final boolean isDemoXML = false;
	private static boolean isTest = false;
	private String environment;
	
	 public ParsingBoltDC(String systemProperty){
		 super();
		 environment = systemProperty;
	 }

	@Override
	public void execute(Tuple input) {
		countMetric.scope("incoming_tuples").incr();
		// UpdateMemberPrompts
		String message = (String) input.getValueByField("str");
		if (message.contains("GetMemberPromptsReply")) {
			// System.out.println("Got a response: ");
			// System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			countMetric.scope("prompts_reply").incr();
			try {
				parseIncomingMessage(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
			 //emitFakeData();

		}
		outputCollector.ack(input);
	}

	void initMetrics(TopologyContext context) {
		countMetric = new MultiCountMetric();
		context.registerMetric("custom_metrics", countMetric, Constants.METRICS_INTERVAL);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		System.setProperty(MongoNameConstants.IS_PROD, environment);
		   HostPortUtility.getInstance(stormConf.get("nimbus.host").toString());
		memberDCDao = new MemberDCDao();
		dc = new DCDao();
		initMetrics(context);
		LOGGER.info("DC Bolt Preparing to Launch");
		varValueType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("score_stream", new Fields("l_id", "lineItemAsJsonString", "source"));
		declarer.declareStream("persist_stream", new Fields("l_id", "lineItemAsJsonString", "source"));
	}

	public void parseIncomingMessage(String message) throws ParserConfigurationException, SAXException, IOException, JSONException {
		JSONObject obj = new JSONObject(message);
		message = (String) obj.get("xmlRespData");
		if(isDemoXML){
			message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID></soapenv:Body></soapenv:Envelope>"; 
		}
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser saxParser = factory.newSAXParser();
		DCParserHandler handler = new DCParserHandler();
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		List<JSONObject> answers = handler.getAnswerList();
		String memberId = handler.getMemberId();
		if(answers != null && answers.size() > 0 && memberId != null){
			processList(answers, memberId);
		}
	}
	
	protected Double processList(List<JSONObject> answers, String memberId) throws JSONException {
		String l_id = SecurityUtils.hashLoyaltyId(memberId);
		if(isTestL_ID){
			l_id = "hzuzVKVINbBBen+WGYQT/VJVdwI=";
		}
		boolean hasStrength = false;
		Double strength_sum = 0.0;
		String category = null;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String date = simpleDateFormat.format(new Date());
		Object varName = null;
		for (int i = 0; i < answers.size(); i++) {
			category = (String) answers.get(i).get("promptGroupName");
			Object strength = dc.getStrength(category, (String) (answers.get(i).get("q_id")), (String) (answers.get(i).get("a_id")));
			varName = dc.getVarName(category);
			if (strength != null && varName != null) {
				strength_sum += JsonUtils.convertToDouble(strength);
				if(!hasStrength){
					hasStrength = true;
				}	
			}
		}
		
		if(hasStrength && !isTest){
			//System.out.println(memberId+" : "+answers);
			emitToPersistStream(date, category, strength_sum, l_id);
			emitToScoreStream(date, category, strength_sum, l_id, (String)varName);
		}
		return strength_sum;
	}
	
	protected void setDCDao(){
		dc = new DCDao();
	}
	
	public void emitToPersistStream(String date, String category, Double strength_sum, String l_id) throws JSONException{
		JSONObject json = new JSONObject();
		json.put("d", date);
		JSONObject dcObject = new JSONObject();
		dcObject.put(category, strength_sum);
		json.put("dc", dcObject);
		List<Object> listToEmit_p = new ArrayList<Object>();
		listToEmit_p.add(l_id);
		listToEmit_p.add(json.toString());
		listToEmit_p.add("DC");
		outputCollector.emit("persist_stream", listToEmit_p);
		LOGGER.info("Emitted message to persist stream");
	}
	
	public void emitToScoreStream(String date, String category, Double strength_sum, String l_id, String varName) throws JSONException{
		Map<String, String> dateDCMap = memberDCDao.getDateStrengthMap(category, l_id);
		String today_str = dateDCMap.get(date);	
		if( today_str == null){
			//put strength_sum
			JSONObject obj = new JSONObject();
			obj.put(category, strength_sum);
			dateDCMap.put(date, obj.toString());
		}else{
			Double today_val = JsonUtils.convertToDouble(JSON.parse(today_str));
			//put val + strength_sum
			today_val+=strength_sum;
			dateDCMap.put(date, today_val.toString());
		}
		Map<String, String> map = new HashMap<String, String>();
		map.put((String)varName, dateDCMap.toString());
		List<Object> listToEmit_s = new ArrayList<Object>();
		listToEmit_s.add(l_id);
		listToEmit_s.add(JsonUtils.createJsonFromStringStringMap(map));
		listToEmit_s.add("DC");
		outputCollector.emit("score_stream", listToEmit_s);
		countMetric.scope("emitted_to_scoring").incr();
		LOGGER.info("Emitted message to score stream");
		
	}
	
	protected void setToTestMode(){
		isTest = true;
	}
}
