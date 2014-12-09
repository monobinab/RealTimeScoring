package analytics.bolt;

import static org.junit.Assert.*;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;
import org.codehaus.jettison.json.JSONException;

public class ParsingBoltDCTest {
	ParsingBoltDC bolt;

	@Before
	public void setUp() throws Exception {
		bolt = new ParsingBoltDC();
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
	
	//TODO:
	//Multi questions tests

}
