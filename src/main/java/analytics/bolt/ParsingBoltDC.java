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
import analytics.util.MongoNameConstants;
import analytics.util.SecurityUtils;
import analytics.util.dao.DCDao;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class ParsingBoltDC extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(ParsingBoltDC.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	private DCDao dc;
	private Type varValueType;
	private MultiCountMetric countMetric;

	@Override
	public void execute(Tuple input) {
		countMetric.scope("incoming_tuples").incr();
		// UpdateMemberPrompts
		String message = (String) input.getValueByField("str");
		if (message.contains("GetMemberPromptsReply")) {
			// System.out.println("Got a response: ");
			// System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			countMetric.scope("dc_PromptsReply").incr();
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

	private void emitFakeData() {
		List<Object> listToEmit = new ArrayList<Object>();
		Gson gson = new Gson();
		HashMap<String, String> variableValueMap = new HashMap<String, String>();
		HashMap<String, List<String>> strengthMap = new HashMap<String, List<String>>();
		List<String> list = new ArrayList<String>();
		list.add("1");
		strengthMap.put("current", list);
		variableValueMap.put("BOOST_DC_APPLIANCE_SSUM", gson.toJson(strengthMap, varValueType));

		// {"BOOST_SYW_WANT_HA_ALL_TCOUNT":"{\"current\":[\"04254571000P\"]}"}
		String varValueString = gson.toJson(variableValueMap, varValueType);
		listToEmit.add("dxo0b7SN1eER9shCSj0DX+eSGag=");
		listToEmit.add(varValueString);
		listToEmit.add("DC");
		LOGGER.info("Emitted message");
		outputCollector.emit("persist_stream",listToEmit);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
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
		message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID></soapenv:Body></soapenv:Envelope>";

		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser saxParser = factory.newSAXParser();
		DefaultHandler handler = new DefaultHandler() {
			boolean bq_id = false;
			boolean ba_id = false;
			boolean bp_id = false;
			boolean bm_id = false;
			String q_id = null;
			String a_id = null;
			String promptGroupName = null;
			String memberId = null;
			List<JSONObject> answers = new ArrayList<JSONObject>();

			int bq = 0;
			int ba = 0;
			int bp = 0;
			int bm = 0;

			public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

				if (qName.contains("QuestionTextID") && !qName.contains("FollowupQuestionTextID")) {
					bq_id = true;
					bq++;
				}

				if (qName.contains("AnswerID") && !qName.contains("FollowupAnswerID")) {
					ba_id = true;
					ba++;

				}

				if (qName.contains("PromptGroupName")) {
					bp_id = true;
					bp++;
				}

				if (qName.contains("MemberNumber")) {
					bm_id = true;
					bm++;
				}

			}

			public void characters(char ch[], int start, int length) throws SAXException {
				String str = new String(ch, start, length);
				if (bq_id) {
					q_id = str;// "bb3300163e00123e11e4211b3aa234e0";
					bq_id = false;
				}

				if (ba_id) {
					a_id = str; // "bb3300163e00123e11e4211b3aa34650";
					ba_id = false;
				}

				if (bp_id) {
					promptGroupName = str; // "DC_Appliance";
					bp_id = false;
				}

				if (bm_id) {
					memberId = str;
					bm_id = false;
				}

				if (q_id != null && a_id != null && promptGroupName != null && memberId != null) {
					JSONObject obj = new JSONObject();
					try {
						obj.put("promptGroupName", promptGroupName);
						obj.put("memberId", memberId);
						obj.put("q_id", q_id);
						obj.put("a_id", a_id);
						answers.add(obj);
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					clear();
				}
			}

			public void clear() {
				q_id = null;
				a_id = null;
				bq_id = false;
				ba_id = false;
			}

			public void endDocument() throws SAXException {

				if (ba > 1 && bq > 1 && answers.size() < 2) {
					System.err.println("We are missing it! 2");
				} else if (ba > 1 && bq > 1 && answers.size() > 1) {
					//System.out.println("We scored it!");
					//System.out.println(answers);

				} else if (ba > 1) {
					System.err.println("We are missing it! 1");
				}
				if(answers.size() > 0){
					countMetric.scope("dc_ValidReply").incr();
					try {
						processList(answers, memberId);
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				q_id = null;
				a_id = null;
				promptGroupName = null;
				memberId = null;
			}

			
		};
		saxParser.parse(new InputSource(new StringReader(message)), handler);
	}
	
	private void processList(List<JSONObject> answers, String memberId) throws JSONException {
		Gson gson = new Gson();
		JSONObject json = new JSONObject();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		json.put("d", simpleDateFormat.format(new Date()));
		List<Object> dclist = new ArrayList<Object>();
		for (int i = 0; i < answers.size(); i++) {
			Map<String, String> variableValueMap = new HashMap<String, String>();
			Object strength = dc.getStrength((String) (answers.get(i).get("promptGroupName")), (String) (answers.get(i).get("q_id")), (String) (answers.get(i).get("a_id")));
			Object varName = dc.getVarName((String) (answers.get(i).get("promptGroupName")));
			if (strength != null && varName != null) {
				HashMap<String, List<Integer>> strengthMap = new HashMap<String, List<Integer>>();
				List<Integer> list = new ArrayList<Integer>();
				list.add((Integer) strength);
				strengthMap.put("current", list);
				variableValueMap.put(varName.toString(), gson.toJson(strengthMap, varValueType));
				String varValueString = gson.toJson(variableValueMap, varValueType);
				List<Object> listToEmit = new ArrayList<Object>();
				listToEmit.add(SecurityUtils.hashLoyaltyId(memberId));
				listToEmit.add(varValueString);
				listToEmit.add("DC");
				outputCollector.emit("score_stream", listToEmit);
				countMetric.scope("dc_EmittedToScoring").incr();
				
				JSONObject dcObj = new JSONObject();
				dcObj.put("c", (String) (answers.get(i).get("promptGroupName")));
				dcObj.put("s", strength);
				dclist.add(dcObj);
				LOGGER.info("Emitted message");
			}
		}
		if(answers.size() > 0){
			json.put("dc", dclist);
			List<Object> listToEmit = new ArrayList<Object>();
			listToEmit.add(SecurityUtils.hashLoyaltyId(memberId));
			listToEmit.add(json.toString());
			listToEmit.add("DC");
			outputCollector.emit("persist_stream", listToEmit);
		}
	}
}
