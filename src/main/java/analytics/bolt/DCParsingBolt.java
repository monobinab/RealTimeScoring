package analytics.bolt;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;

import analytics.util.SecurityUtils;
import analytics.util.dao.DCDao;
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

public class DCParsingBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(DCParsingBolt.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;
	private DCDao dc;
	private Type varValueType;

	@Override
	public void execute(Tuple input) {
		
		// UpdateMemberPrompts
		String message = (String) input.getValueByField("str");
		if (message.contains("GetMemberPromptsReply")) {
//			System.out.println("Got a response: ");
//			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			try {
				parseIncomingMessage(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
			//emitFakeData();
			outputCollector.ack(input);
		} else {
			outputCollector.ack(input);
		}
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

		//{"BOOST_SYW_WANT_HA_ALL_TCOUNT":"{\"current\":[\"04254571000P\"]}"}
		String varValueString = gson.toJson(variableValueMap, varValueType);
		listToEmit.add("dxo0b7SN1eER9shCSj0DX+eSGag=");
		listToEmit.add(varValueString);
		listToEmit.add("DC");
		LOGGER.info("Emitted message");
		outputCollector.emit(listToEmit);
	}

	

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.outputCollector = collector;
		dc = new DCDao();
		LOGGER.info("DC Bolt Preparing to Launch");
		varValueType = new TypeToken<Map<String, String>>() {
			private static final long serialVersionUID = 1L;
		}.getType();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source"));
	}

	public void parseIncomingMessage(String message) throws ParserConfigurationException, SAXException, IOException, JSONException {
		JSONObject obj = new JSONObject(message);
		message = (String) obj.get("xmlRespData");
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser saxParser = factory.newSAXParser();
		//TODO: Handle cases where there are multiple questions answered
		DefaultHandler handler = new DefaultHandler() {
			boolean bq_id = false;
			boolean ba_id = false;
			boolean bp_id = false;
			boolean bm_id = false;
			String q_id = null;
			String a_id = null;
			String promptGroupName = null;
			String memberId = null;

			public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

				if (qName.contains("QuestionTextID") && !qName.contains("FollowupQuestionTextID")) {
					bq_id = true;
				}

				if (qName.contains("AnswerID") && !qName.contains("FollowupAnswerID")) {
					ba_id = true;
				}

				if (qName.contains("PromptGroupName")) {
					bp_id = true;
				}
				
				if(qName.contains("MemberNumber")){
					bm_id = true;
				}

			}

			public void characters(char ch[], int start, int length) throws SAXException {
				String str = new String(ch, start, length);
				if (bq_id) {
					q_id = str;
					bq_id = false;
				}

				if (ba_id) {
					a_id = str;
					ba_id = false;
				}

				if (bp_id) {
					promptGroupName = str;
					bp_id = false;
				}
				
				if(bm_id){
					memberId = str;
					bm_id = false;
				}
			}

			public void endDocument() throws SAXException {
				Map<String, String> variableValueMap = new HashMap<String, String>();
				if (q_id != null && a_id != null && promptGroupName != null && memberId != null) {
//					System.out.println("Member ID : " + memberId);
//					System.out.println("QuestionId : " + q_id);
//					System.out.println("AnswerId : " + a_id);
//					System.out.println("PromptGroupName : " + promptGroupName);
					Object strength = dc.getStrength(promptGroupName, q_id, a_id);
					Object varName = dc.getVarName(promptGroupName);
					if (strength != null && varName != null) {
						variableValueMap.put(varName.toString(), strength.toString());
						Gson gson = new Gson();
						String varValueString = gson.toJson(variableValueMap, varValueType);
						List<Object> listToEmit = new ArrayList<Object>();
						listToEmit.add(SecurityUtils.hashLoyaltyId(memberId));
						listToEmit.add(varValueString);
						listToEmit.add("DC");
						outputCollector.emit(listToEmit);
						LOGGER.info("Emitted message");
						//System.err.println("yay");
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

}
