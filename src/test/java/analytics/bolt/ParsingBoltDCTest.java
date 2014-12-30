package analytics.bolt;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import analytics.util.DBConnection;
import analytics.util.DCParserHandler;
import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import analytics.util.dao.DCDao;

public class ParsingBoltDCTest {
	static ParsingBoltDC bolt;
	static SAXParserFactory factory;
	static SAXParser saxParser;
	static DCParserHandler handler;
	static Map<String, String> conf;
	static DBCollection dcQAStrength;
	static DBCollection dcModels;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		bolt = new ParsingBoltDC();
		bolt.setToTestMode();
		factory = SAXParserFactory.newInstance();
		saxParser = factory.newSAXParser();
		handler = new DCParserHandler();

		System.setProperty(MongoNameConstants.IS_PROD, "test");
		conf = new HashMap<String, String>();
		conf.put("rtseprod", "test");
		DB db = FakeMongo.getTestDB();
		bolt.setDB(db);
		dcQAStrength = db.getCollection(MongoNameConstants.DC_QA_STRENGTHS);
		dcModels = db.getCollection(MongoNameConstants.DC_MODEL);
		populateDCCollections();
		DBObject query = new BasicDBObject();
		query.put("c", "DC_1");
		query.put("q", "q_id_1");
		query.put("a", "a_id_1");
		DBObject result = (DBObject) dcQAStrength.findOne(query);
	}

	private static void populateDCCollections() {
		int num_rec = 3;

		for (int i = 1; i < num_rec; i++) {
			addRecordForDCModel(i);
			addRecordForQAStrength(i);
		}
	}

	private static void addRecordForDCModel(int s) {
		DBObject strengthRecord = new BasicDBObject();
		strengthRecord.put("d", "DC_" + s);
		strengthRecord.put("v", "VAR_" + s);
		List<Integer> m = new ArrayList<Integer>();
		m.add(34);
		strengthRecord.put("m", m);
		dcModels.insert(strengthRecord);
	}

	private static void addRecordForQAStrength(int s) {
		DBObject strengthRecord = new BasicDBObject();
		strengthRecord.put("q", "q_id_" + s);
		strengthRecord.put("a", "a_id_" + s);
		strengthRecord.put("c", "DC_" + s);
		strengthRecord.put("s", s);
		dcQAStrength.insert(strengthRecord);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected = org.codehaus.jettison.json.JSONException.class)
	public void parseIncomingMessageTest1() throws ParserConfigurationException, SAXException, IOException, JSONException {
		String testMessage_NoneJSON = "jiowejr][";
		bolt.parseIncomingMessage(testMessage_NoneJSON);
	}

	@Test(expected = org.codehaus.jettison.json.JSONException.class)
	public void parseIncomingMessageTest2() throws ParserConfigurationException, SAXException, IOException, JSONException {
		String testMessage_DummyJSON = "{\"hello\":\"world\"}";
		bolt.parseIncomingMessage(testMessage_DummyJSON);
	}

	@Test(expected = org.xml.sax.SAXException.class)
	public void parseIncomingMessageTest3() throws ParserConfigurationException, SAXException, IOException, JSONException {
		String testMessage_DummyJSONWithxmlRespData_NonXML = "{\"xmlRespData\":\"world\"}";
		bolt.parseIncomingMessage(testMessage_DummyJSONWithxmlRespData_NonXML);
	}

	@Test
	public void parseIncomingMessageTest4() throws ParserConfigurationException, SAXException, IOException, JSONException {
		String testMessage_DummyJSONWithxmlRespData = "{\"xmlRespData\":\"<properties><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding><slf4j-version>1.7.2</slf4j-version><storm.version>0.9.0.1</storm.version></properties>\"}";
		bolt.parseIncomingMessage(testMessage_DummyJSONWithxmlRespData);
	}

	// TODO: cover the missing branches in ParserHandler

	@Test
	public void DCParserHandlerTestMulti() throws SAXException, IOException, JSONException {

		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID>bb3300163e00123e11e4211b3ae0eb90</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3ae07660</QuestionTextID><QuestionTextID>bb3300163e00123e11e4211b3cb81c90</QuestionTextID><AnswerID>bb3300163e00123e11e4211b3cb92e00</AnswerID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(3, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
		assertEquals("bb3300163e00123e11e4211b3aa34650", (String) answers.get(0).get("a_id"));
		assertEquals("bb3300163e00123e11e4211b3ae07660", (String) answers.get(1).get("q_id"));
		assertEquals("bb3300163e00123e11e4211b3ae0eb90", (String) answers.get(1).get("a_id"));
		assertEquals("bb3300163e00123e11e4211b3cb81c90", (String) answers.get(2).get("q_id"));
		assertEquals("bb3300163e00123e11e4211b3cb92e00", (String) answers.get(2).get("a_id"));
		assertEquals("DC_Appliance", (String) answers.get(0).get("promptGroupName"));
	}

	@Test
	public void DCParserHandlerTestDuplicates0() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><QuestionTextID></QuestionTextID><AnswerID></AnswerID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(1, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
	}

	@Test
	public void DCParserHandlerTestDuplicates1() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><QuestionTextID>a</QuestionTextID><AnswerID></AnswerID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(1, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
	}

	@Test
	public void DCParserHandlerTestDuplicates2() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><QuestionTextID></QuestionTextID><AnswerID>a1</AnswerID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(1, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
	}

	@Test
	public void DCParserHandlerTestDuplicates3() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><QuestionTextID>a</QuestionTextID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(1, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
	}

	@Test
	public void DCParserHandlerTestDuplicates4() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><QuestionTextID></QuestionTextID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(1, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
	}

	@Test
	public void DCParserHandlerTestDuplicates5() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID>a</AnswerID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(1, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
	}

	@Test
	public void DCParserHandlerTestDuplicates6() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID></AnswerID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(1, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
	}

	@Test
	public void DCParserHandlerTestMalform0() throws SAXException, IOException, JSONException {
		String message = "<soapenv:Envelope xmlns:soapenv=\"http://www.w3.org/2003/05/soap-envelope\"><soapenv:Body><PromptGroupName>DC_Appliance</PromptGroupName><MemberNumber>fake</MemberNumber><AnswerID>bb3300163e00123e11e4211b3aa34650</AnswerID><QuestionTextID>bb3300163e00123e11e4211b3aa234e0</QuestionTextID><AnswerID></AnswerID><QuestionTextID>q</QuestionTextID><AnswerID>a</AnswerID></soapenv:Body></soapenv:Envelope>";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		assertEquals("fake", handler.getMemberId());
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(2, answers.size());
		assertEquals("bb3300163e00123e11e4211b3aa234e0", (String) answers.get(0).get("q_id"));
		assertEquals("a", (String) answers.get(1).get("a_id"));
	}

	@Test
	public void DCParserHandlerTestPromptWithFollowUps() throws SAXException, IOException, JSONException {
		String message = "\u003csoap:Envelope xmlns:soap\u003d\"http://www.w3.org/2003/05/soap-envelope\" xmlns:xsi\u003d\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd\u003d\"http://www.w3.org/2001/XMLSchema\"\u003e\u003csoap:Body\u003e\u003cns2:GetMemberPromptsResponse xmlns:ns3\u003d\"http://www.epsilon.com/webservices/common\" xmlns:ns2\u003d\"http://www.epsilon.com/webservices/\"\u003e\u003cns2:GetMemberPromptsResult\u003e\u003cns2:Epsilon\u003e\u003cns2:MTServerName\u003etrprtel2app06\u003c/ns2:MTServerName\u003e\u003cns2:Response Type\u003d\"PROCESSED\"\u003e\u003cns2:GetMemberPromptsReply\u003e\u003cns2:ResponseDate\u003e2014-12-14\u003c/ns2:ResponseDate\u003e\u003cns2:ResponseTime\u003e13:24:00\u003c/ns2:ResponseTime\u003e\u003cns2:MemberNumber\u003e7081121043254318\u003c/ns2:MemberNumber\u003e\u003cns2:NumQuestionsReturned\u003e1\u003c/ns2:NumQuestionsReturned\u003e\u003cns2:CheckoutPrompt\u003e\u003cns2:CheckoutPromptQuestions\u003e\u003cns2:PromptGroupName\u003eDC_Apparel\u003c/ns2:PromptGroupName\u003e\u003cns2:QuestionPriorityNum\u003e1215.0\u003c/ns2:QuestionPriorityNum\u003e\u003cns2:AttributeID\u003eBB3300163E00123E11E4211D06A25E71\u003c/ns2:AttributeID\u003e\u003cns2:QuestionPackageID\u003eBB3300163E00123E11E4211D06A2AC90\u003c/ns2:QuestionPackageID\u003e\u003cns2:QuestionRuleID\u003eBB3300163E00123E11E4211D06A0B0C0\u003c/ns2:QuestionRuleID\u003e\u003cns2:QuestionTextID\u003eBB3300163E00123E11E4211D06A2AC90\u003c/ns2:QuestionTextID\u003e\u003cns2:QuestionTitle\u003eApparel\u003c/ns2:QuestionTitle\u003e\u003cns2:QuestionLine1\u003eWhat size(s) of activewear are you interested in? (Please select all that apply)\u003c/ns2:QuestionLine1\u003e\u003cns2:QuestionLine2\u003e\u003c/ns2:QuestionLine2\u003e\u003cns2:QuestionLine3\u003e\u003c/ns2:QuestionLine3\u003e\u003cns2:QuestionLine4\u003e\u003c/ns2:QuestionLine4\u003e\u003cns2:QuestionLine5\u003e\u003c/ns2:QuestionLine5\u003e\u003cns2:AnswerTemplate\u003eMultiChoice\u003c/ns2:AnswerTemplate\u003e\u003cns2:AnswerChoices\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A321C0\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eXXS\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eXXS\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A321C1\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eXS\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eXS\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A348D0\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eS\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eS\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A348D1\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eM\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eM\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A36FE0\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eL\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eL\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A36FE1\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eXL\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eXL\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A396F0\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eXXL\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eXXL\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A3BE00\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eXXXL\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eXXXL\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A3BE01\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eOther\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eOther\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003cns2:AnswerOption\u003e\u003cns2:AnswerChoiceID\u003eBB3300163E00123E11E4211D06A40C20\u003c/ns2:AnswerChoiceID\u003e\u003cns2:ButtonTxt\u003eNot applicable\u003c/ns2:ButtonTxt\u003e\u003cns2:AnswerTxt\u003eNot applicable\u003c/ns2:AnswerTxt\u003e\u003c/ns2:AnswerOption\u003e\u003c/ns2:AnswerChoices\u003e\u003cns2:ApiToUse\u003eUpdateMemberPrompts\u003c/ns2:ApiToUse\u003e\u003cns2:MemberAttribute\u003eAdditionalAttributes\u003c/ns2:MemberAttribute\u003e\u003cns2:AdditionalAttrName\u003eAttr_AnalyApparel208\u003c/ns2:AdditionalAttrName\u003e\u003cns2:FollowupQuestions\u003e\u003cns2:FollowupQuestion\u003e\u003cns2:FollowupQuestionTitle\u003eApparel\u003c/ns2:FollowupQuestionTitle\u003e\u003cns2:QuestionToFollowup\u003eBB3300163E00123E11E4211D06A2AC90\u003c/ns2:QuestionToFollowup\u003e\u003cns2:AnswerstoFollowup\u003e\u003cns2:Answer\u003e\u003cns2:AnswerID\u003eBB3300163E00123E11E4211D06A3BE01\u003c/ns2:AnswerID\u003e\u003c/ns2:Answer\u003e\u003c/ns2:AnswerstoFollowup\u003e\u003cns2:FollowupQuestionTextID\u003eBB3300163E00123E11E4211D06A2FAB0\u003c/ns2:FollowupQuestionTextID\u003e\u003cns2:FollowupAttributeID\u003eBB3300163E00123E11E4211D06A25E71\u003c/ns2:FollowupAttributeID\u003e\u003cns2:FollowupQuestionLine1\u003eIf Other, what size(s) of activewear are you interested in?\u003c/ns2:FollowupQuestionLine1\u003e\u003cns2:FollowupQuestionLine2\u003e\u003c/ns2:FollowupQuestionLine2\u003e\u003cns2:FollowupQuestionLine3\u003e\u003c/ns2:FollowupQuestionLine3\u003e\u003cns2:FollowupQuestionLine4\u003e\u003c/ns2:FollowupQuestionLine4\u003e\u003cns2:FollowupQuestionLine5\u003e\u003c/ns2:FollowupQuestionLine5\u003e\u003cns2:FollowupAnswerTemplate\u003eFreeText\u003c/ns2:FollowupAnswerTemplate\u003e\u003cns2:FollowupAnswerChoices\u003e\u003cns2:FollowupAnswerChoice\u003e\u003cns2:FollowupAnswerID\u003eBB3300163E00123E11E4211D06A40C21\u003c/ns2:FollowupAnswerID\u003e\u003cns2:FollowupButtonText\u003e\u003c/ns2:FollowupButtonText\u003e\u003cns2:FollowupAnswerText\u003e\u003c/ns2:FollowupAnswerText\u003e\u003c/ns2:FollowupAnswerChoice\u003e\u003c/ns2:FollowupAnswerChoices\u003e\u003cns2:FollowupMemberAttribute\u003eAttr_AnalyApparel208\u003c/ns2:FollowupMemberAttribute\u003e\u003cns2:FollowupAttrName\u003eAttr_AnalyApparel208\u003c/ns2:FollowupAttrName\u003e\u003c/ns2:FollowupQuestion\u003e\u003c/ns2:FollowupQuestions\u003e\u003c/ns2:CheckoutPromptQuestions\u003e\u003c/ns2:CheckoutPrompt\u003e\u003c/ns2:GetMemberPromptsReply\u003e\u003c/ns2:Response\u003e\u003cns2:Additional\u003e\u003cns2:Status\u003e00\u003c/ns2:Status\u003e\u003cns2:StatusText\u003eSuccess\u003c/ns2:StatusText\u003e\u003cns2:MessageVersion\u003e01\u003c/ns2:MessageVersion\u003e\u003cns2:SysPulse\u003e44ms\u003c/ns2:SysPulse\u003e\u003c/ns2:Additional\u003e\u003c/ns2:Epsilon\u003e\u003c/ns2:GetMemberPromptsResult\u003e\u003c/ns2:GetMemberPromptsResponse\u003e\u003c/soap:Body\u003e\u003c/soap:Envelope\u003e";
		saxParser.parse(new InputSource(new StringReader(message)), handler);
		List<JSONObject> answers = handler.getAnswerList();
		assertEquals(2, answers.size());
		assertEquals((String)answers.get(1).get("a_id"), "BB3300163E00123E11E4211D06A40C21");
		assertEquals((String)answers.get(1).get("q_id"), "BB3300163E00123E11E4211D06A2FAB0");
		assertEquals("BB3300163E00123E11E4211D06A2AC90",(String)answers.get(0).get("q_id"));
		assertEquals("BB3300163E00123E11E4211D06A3BE01", (String)answers.get(0).get("a_id"));
	}

	@Test
	public void processListTest0() throws JSONException {
		String memberID = "test";
		List<JSONObject> answers = createAnswerList(memberID, 0);
		Double result = bolt.processList(answers, memberID);
		double expected = 1.0;
		assertEquals(expected, (double) result, 0.0);
	}
	
	@Test
	public void processListTest1() throws JSONException {
		String memberID = "test";
		List<JSONObject> answers = createAnswerList(memberID, 1);
		Double result = bolt.processList(answers, memberID);
		double expected = 0.0;
		assertEquals(expected, (double) result, 0.0);
	}
	
	@Test
	public void processListTest2() throws JSONException {
		String memberID = "test";
		List<JSONObject> answers = createAnswerList(memberID, 2);
		Double result = bolt.processList(answers, memberID);
		double expected = 0.0;
		assertEquals(expected, (double) result, 0.0);
	}
	
	@Test
	public void processListTest3() throws JSONException {
		String memberID = "test";
		List<JSONObject> answers = createAnswerList(memberID, 3);
		Double result = bolt.processList(answers, memberID);
		double expected = 2.0;
		assertEquals(expected, (double) result, 0.0);
	}

	private List<JSONObject> createAnswerList(String memberID, int test_num) throws JSONException {
		JSONObject obj = new JSONObject();
		List<JSONObject> answers = new ArrayList<JSONObject>();
		switch (test_num) {
		case 0:
			obj.put("promptGroupName", "DC_1");
			obj.put("memberId", memberID);
			obj.put("q_id", "Q_ID_1");
			obj.put("a_id", "A_ID_1");
			answers.add(obj);
			return answers;
		case 1:
			obj.put("promptGroupName", "DC_3");
			obj.put("memberId", memberID);
			obj.put("q_id", "Q_ID_1");
			obj.put("a_id", "A_ID_1");
			answers.add(obj);
			return answers;
		case 2:
			obj.put("promptGroupName", "DC_1");
			obj.put("memberId", memberID);
			obj.put("q_id", "Q_ID_1");
			obj.put("a_id", "A_ID_0");
			answers.add(obj);
			return answers;
		case 3:
			obj.put("promptGroupName", "DC_2");
			obj.put("memberId", memberID);
			obj.put("q_id", "Q_ID_2");
			obj.put("a_id", "A_ID_2");
			answers.add(obj);
			return answers;
		}
		
		return null;
	}

	// TODO: flip all those expected placed to actual
	// TODO: place all helper methods on top

}
