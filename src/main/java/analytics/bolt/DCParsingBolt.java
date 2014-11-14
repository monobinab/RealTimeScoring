package analytics.bolt;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import analytics.util.SecurityUtils;
import analytics.util.dao.DCDao;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.commons.collections.map.HashedMap;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class DCParsingBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(DCParsingBolt.class);
	private static final long serialVersionUID = 1L;
	private static OutputCollector collector;
	private static DCDao dc;
	private Type varValueType;

	@Override
	public void execute(Tuple message) {

		// UpdateMemberPrompts
		String input = message.toString();
		if (input.contains("GetMemberPromptsReply")) {

			System.out.println(": Got a response");
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			try {
				parseIncomingMessage("*", message);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public void parseIncomingMessage(String tagName, Tuple message) {
		try {
			//Get the loyalty id from the message
			//Call sEcurityUtils.hashLoyaltyId to get the l_id

			String str = (String) message.getValueByField("str");
			JSONObject obj = new JSONObject(str);
			Document doc = loadXMLFromString((String) obj.get("xmlRespData"));
			String q_id = null;
			String a_id = null;
			String category = null;
			NodeList elements = doc.getElementsByTagName(tagName);
			if (elements == null || elements.getLength() <= 0) {
				return;
			}
			for (int i = 0; i < elements.getLength(); i++) {
				Element node = (Element) elements.item(i);
				if (node.getChildNodes() != null && node.getChildNodes().getLength() > 0 && node.getChildNodes().item(0) != null) {
					// map.put(node.getTagName(),
					// node.getChildNodes().item(0).getNodeValue());
					String key = node.getTagName();
					String value = node.getChildNodes().item(0).getNodeValue();
					if (key.contains("QuestionTextID") && !key.contains("FollowupQuestionTextID")) {
						q_id = value;
						System.out.println(key + " : " + value);
					}
					if (key.contains("AnswerID") && !key.contains("FollowupAnswerID")) {
						a_id = value;
						System.out.println(key + " : " + value);
					}
					if (key.contains("PromptGroupName")) {
						category = value;
						System.out.println(key + " : " + value);
					}

				}

			}
			Map<String,String> variableValueMap = new HashMap<String, String>();
			if (q_id != null && a_id != null && category != null) {
				Object strength = dc.getStrength(category, q_id, a_id);
				if (strength != null){
					variableValueMap.put("VARNAME", strength.toString());//Get varname from dcmodel collection
					Gson gson = new Gson();
					String varValueString = gson.toJson(variableValueMap, varValueType);
					List<Object> listToEmit = new ArrayList<Object>();
					listToEmit.add("l_id");//TODO: Get the lid
					listToEmit.add(varValueString);
					listToEmit.add("DC");
				}
				LOGGER.info("Emitted message");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Document loadXMLFromString(String xml) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		InputSource is = new InputSource(new StringReader(xml));
		Document doc = builder.parse(is);
		return doc;
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		dc = new DCDao();
		LOGGER.info("DC Bolt Preparing to Launch");
		 Type varValueType = new TypeToken<Map<String, String>>() {
				private static final long serialVersionUID = 1L;
			}.getType();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("l_id", "lineItemAsJsonString", "source"));
	}

}
