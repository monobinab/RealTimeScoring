package analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import analytics.util.dao.DivLineBuSubDao;
import analytics.util.dao.EventsVibesActiveDao;
import analytics.util.dao.HoldMembersDAO;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.OccasionResponsesDao;
import analytics.util.dao.OccationCustomeEventDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagResponsysActiveDao;
import analytics.util.dao.TagVariableDao;
import analytics.util.dao.XMLResponsysDAO;
import analytics.util.objects.MemberInfo;
import analytics.util.objects.ResponsysPayload;
import analytics.util.objects.TagMetadata;
import backtype.storm.metric.api.MultiCountMetric;

public class ResponsysUtil {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponsysUtil.class);
	private TagMetadataDao tagMetadataDao;
	private OccationCustomeEventDao occationCustomeEventDao;
	private OccasionResponsesDao occasionResponsesDao;
	private MemberInfoDao memberInfoDao;
	private TagVariableDao tagVariableDao;
	private Map<Integer, String> tagModelsMap;
	private HashSet<String> activeTags = new HashSet<String>();
	private DivLineBuSubDao divLineBuSubDao;
	private static final String UTF8_BOM = "\uFEFF";
	private TagResponsysActiveDao tagResponsysActiveDao;
	private static final String validUnownTags = "Top 5% of MSM,Unknown";
	private EventsVibesActiveDao eventsVibesActiveDao;
	private MemberMDTags2Dao memberMDTags2Dao;
	private MemberMDTagsDao memberMDTagsDao;
	private XMLResponsysDAO xmlResponsysDAO;
	private HoldMembersDAO holdMembersDAO;
	private List<String> holdMembers;

	public ResponsysUtil() {
	
		tagMetadataDao = new TagMetadataDao();
		occationCustomeEventDao = new OccationCustomeEventDao();
		occasionResponsesDao = new OccasionResponsesDao();
		tagResponsysActiveDao =  new TagResponsysActiveDao();
		tagVariableDao = new TagVariableDao();
		eventsVibesActiveDao = new EventsVibesActiveDao();
		memberInfoDao = new MemberInfoDao();
		tagVariableDao = new TagVariableDao();
		activeTags = getReadyToProcessTags();
		tagModelsMap = tagVariableDao.getTagModelIds(activeTags);
		divLineBuSubDao = new DivLineBuSubDao();
		memberMDTagsDao = new MemberMDTagsDao();
		memberMDTags2Dao = new MemberMDTags2Dao();
		xmlResponsysDAO=new XMLResponsysDAO();
		holdMembersDAO=new HoldMembersDAO();
		holdMembers=holdMembersDAO.getHoldMembersList();
	}
	
	
	/**
	 * @return the memberMDTags2Dao
	 */
	public MemberMDTags2Dao getMemberMDTags2Dao() {
		return memberMDTags2Dao;
	}


	/**
	 * @param memberMDTags2Dao the memberMDTags2Dao to set
	 */
	public void setMemberMDTags2Dao(MemberMDTags2Dao memberMDTags2Dao) {
		this.memberMDTags2Dao = memberMDTags2Dao;
	}


	/**
	 * @return the memberMDTagsDao
	 */
	public MemberMDTagsDao getMemberMDTagsDao() {
		return memberMDTagsDao;
	}


	/**
	 * @param memberMDTagsDao the memberMDTagsDao to set
	 */
	public void setMemberMDTagsDao(MemberMDTagsDao memberMDTagsDao) {
		this.memberMDTagsDao = memberMDTagsDao;
	}


	public  Map<Integer, String> getTagModelsMap(){
		return tagModelsMap;
	}
	public TagMetadataDao getTagMetadataDao() {
		return tagMetadataDao;
	}
	public void setTagMetadataDao(TagMetadataDao tagMetadataDao) {
		this.tagMetadataDao = tagMetadataDao;
	}
	public OccationCustomeEventDao getOccationCustomeEventDao() {
		return occationCustomeEventDao;
	}
	public void setOccationCustomeEventDao(
			OccationCustomeEventDao occationCustomeEventDao) {
		this.occationCustomeEventDao = occationCustomeEventDao;
	}
	public OccasionResponsesDao getOccasionResponsesDao() {
		return occasionResponsesDao;
	}
	public void setOccasionResponsesDao(OccasionResponsesDao occasionResponsesDao) {
		this.occasionResponsesDao = occasionResponsesDao;
	}
	public MemberInfoDao getMemberInfoDao() {
		return memberInfoDao;
	}
	public void setMemberInfoDao(MemberInfoDao memberInfoDao) {
		this.memberInfoDao = memberInfoDao;
	}
	public TagVariableDao getTagVariableDao() {
		return tagVariableDao;
	}
	public void setTagVariableDao(TagVariableDao tagVariableDao) {
		this.tagVariableDao = tagVariableDao;
	}
	public TagResponsysActiveDao getTagResponsysActiveDao() {
		return tagResponsysActiveDao;
	}
	public void setTagResponsysActiveDao(TagResponsysActiveDao tagResponsysActiveDao) {
		this.tagResponsysActiveDao = tagResponsysActiveDao;
	}
	public EventsVibesActiveDao getEventsVibesActiveDao() {
		return eventsVibesActiveDao;
	}
	public void setEventsVibesActiveDao(EventsVibesActiveDao eventsVibesActiveDao) {
		this.eventsVibesActiveDao = eventsVibesActiveDao;
	}
	/**
	 * Invokes the RTS web service that returns scores...
	 * 
	 * @param lyl_l_id
	 *            the loyalty Id
	 * @param jsonArr
	 *            the "payload" to the webservice
	 * @return the web service response as a JSON String
	 * @throws IOException
	 */
	public String callRtsAPI(String lyl_l_id, String topologyName) {
	//	String baseURL = Constants.SCORING_API_PRE+lyl_l_id+Constants.SCORING_API_POST;
		String baseURL = Constants.SCORING_API_PRE+lyl_l_id+AuthPropertiesReader.getProperty(topologyName+"PostapiUrl");
		String jsonRespString = null;
		try {
			HttpClient httpclient = new DefaultHttpClient();
			HttpGet httpget = new HttpGet(baseURL);

			LOGGER.debug("executing request " + httpget.getRequestLine());
			HttpResponse response = httpclient.execute(httpget);
			String responseString = response.getStatusLine().toString();
			LOGGER.debug("RTS API Response : " + responseString);
			InputStream instream = response.getEntity().getContent();
			jsonRespString = read(instream);
		} catch (IOException e3) {
			e3.printStackTrace();
			LOGGER.error("IO Exception Occured " + baseURL + "\n" + e3);
			return null;
		} catch (Exception e5) {
			e5.printStackTrace();
			LOGGER.error("Error occured while calling the web service " , e5.getMessage() + "---" +baseURL);
			return null;
		}
		return jsonRespString;
	}

	/**
	 * 
	 * @param in
	 * @return String
	 * @throws IOException
	 */
	private static String read(InputStream in) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader r = new BufferedReader(new InputStreamReader(in), 1000);
		for (String line = r.readLine(); line != null; line = r.readLine())
			sb.append(line);
		in.close();
		return sb.toString();
	}

	/**
	 * Hit the Url and get the response back from Oracle
	 * @param input
	 * @param countMetric 
	 * @return The Tag that was sent to Responsys
	 * @throws Exception
	 */
	public TagMetadata getResponseServiceResult(String input, String lyl_l_id, String l_id, 
			String messageID, MultiCountMetric countMetric) throws Exception {
		LOGGER.info(" Testing - Entering the getResponseServiceResult method");
		TagMetadata winningTag = null;
		try {

			org.json.JSONObject obj = new org.json.JSONObject(input);
			
			//Determine the winner tag to send to Responsys
			//TagMetadata winningTag = determineWinningTag(obj,tags);
			org.json.JSONArray arr = obj.getJSONArray("scoresInfo");
			if(((org.json.JSONObject)arr.get(0)).has("occassion")  && 
					validUnownTags.contains(((org.json.JSONObject)arr.get(0)).get("occassion").toString())){
				winningTag = determineUnknownWinner(arr);	
			}else{
				winningTag = getTagMetaDataInfo(obj);
			}
			
			if(winningTag!=null){
				//Get the necessary variables for populating in the response xml
				LOGGER.debug("TIME:" + messageID + "- Getting EID -" + System.currentTimeMillis());
				MemberInfo memberInfo  = memberInfoDao.getMemberInfo(l_id);
				LOGGER.debug("TIME:" + messageID + "- Got EID -" + System.currentTimeMillis());
				
				//Send to Responsys only when there is member info or when there is an non zero eid
				if(memberInfo==null || memberInfo.getEid() == null || memberInfo.getEid().equals("0")){
					countMetric.scope("null_memberinfo").incr();
					LOGGER.info("PERSIST: No Member Info available for Lid " + lyl_l_id );
					return null;
				}
				
				String custEventName = occationCustomeEventDao.getCustomeEventName(winningTag.getPurchaseOccasion());
	
				//Process the message including sending to Responsys
				processMessage(memberInfo, custEventName, 
						winningTag, lyl_l_id, "PO", l_id);
				
				//winningTag.setEmailOptIn( memberInfo != null?memberInfo.getEmailOptIn():null);
				winningTag.setTextOptIn(memberInfo != null?memberInfo.getText_opt_in():null);

				obj = null;
			}
			//Else just Log the message for the Lid ...
			else{
				LOGGER.info("PERSIST: No Winning Tag found - Not sending to Responsys for Lid " + lyl_l_id );
			}
			
		} catch (Exception t) {
			t.printStackTrace();
			LOGGER.error("Exception occured in getResponseServiceResult ", t);
		} 
		LOGGER.info(" exiting the method getResponseServiceResult");
		return winningTag;
	}


	/**
	 * 
	 * @param responsysObj
	 * @return 
	 * @throws Exception
	 */
	public String getResponsysServiceResult(ResponsysPayload responsysObj) throws Exception {
		LOGGER.info(" Testing - Entering the getResponseUnknownServiceResult method");

		try {
			if(null != responsysObj){
				processMessage(responsysObj.getMemberInfo(), responsysObj.getCustomEventName(), responsysObj.getTagMetadata(), responsysObj.getLyl_id_no(), responsysObj.getTopologyName(), responsysObj.getL_id());
			}
			responsysObj = null;
		} catch (Exception t) {
			t.printStackTrace();
			LOGGER.error("Exception occured in getResponseServiceResult ", t);
		}
			LOGGER.info(" exiting the method getResponseServiceResult");
			return null;
	}
	/**
	 * 
	 * @param obj
	 * @param eid
	 * @param customEventName
	 * @param tagMetadata
	 * @param lyl_l_id
	 * @return The response from Responsys
	 * @throws Exception
	 */
	public void processMessage(MemberInfo memberInfo, String customEventName, 
			TagMetadata tagMetadata, String lyl_l_id, String topologyName, String l_id) throws Exception{
		
		//Generate the Custome Xml to be sent to Oracle
		Long time = System.currentTimeMillis();
		String customXml = createCustomXml("", memberInfo, customEventName, tagMetadata, lyl_l_id, topologyName);
		
		LOGGER.info("Time Taken to create custom xml = " + (System.currentTimeMillis() - time));
	
		//BOM = Byte-Order-Mark
		//Remove the BOM to make the XML valid
		String xmlWithoutBOM = removeUTF8BOM(customXml);

		//If a member is in hold list, skip the responsys 'SEND' process and add the record with 'F' status to XMLResponsys collection
		boolean holdEmail=checkinHoldList(lyl_l_id);
		StringBuffer strBuff=new StringBuffer();
		String emailstatus="N";
		if(!holdEmail)
		{
			strBuff = sendToResponsys(xmlWithoutBOM,memberInfo);
		}
		else
		{
			emailstatus="R";
		}
		//Code to handle timeouts
		if(strBuff.length()==0)
		{
			String hashedXML = createCustomXml("", memberInfo, customEventName, tagMetadata, l_id, topologyName);
			String hashxmlWithoutBOM = removeUTF8BOM(hashedXML);
			xmlResponsysDAO.addXMLResponsys(l_id,hashxmlWithoutBOM,memberInfo.getWinningOptIn(),emailstatus, topologyName);
		}
		
		//timeout code ends
		//Persist info to Mongo after successfully transmission of message to Oracle.
		occasionResponsesDao.addOccasionResponse(l_id, memberInfo.getEid(), customEventName, 
				!topologyName.equalsIgnoreCase("unknownOccasions")?tagMetadata.getPurchaseOccasion():"Unknown", 
						tagMetadata, strBuff.toString().contains("<success>true</success>") ? "Y" : emailstatus, topologyName);
	
		LOGGER.info("PERSIST: Winning Tag for Lid: " + lyl_l_id +" : "+tagMetadata.getMdTag());
		LOGGER.info("Response String ====>" + strBuff.toString());
		strBuff = null;
		LOGGER.info("Time taken in Process Message = " + (System.currentTimeMillis() - time));
	}
	
	private boolean checkinHoldList(String lyl_l_id) {
		
		for(String member: holdMembers) {
		    if(member.trim().contains(lyl_l_id))
		       return true;
		}
		return false;
			
		
	}


	/**
	 * 
	 * @param xml
	 * @return XML String with the exponential values removed from Score and Total Score tags
	 * @throws SAXException
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 */
	public String removeExponentialFromXml(String xml) throws SAXException, IOException, ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException{
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

		InputSource is = new InputSource();
	    is.setCharacterStream(new StringReader(xml));
		Document doc = docBuilder.parse(is);

		NodeList scoresInfoList = doc.getElementsByTagName("scoresInfo");

		for (int i = 0; i < scoresInfoList.getLength(); i++) {

	       Element element = (Element) scoresInfoList.item(i);
	       NodeList name = element.getElementsByTagName("score");
           Element line = (Element) name.item(0);
           Node node = name.item(0);
         
		   // get the score element, and update the value
		   if ("score".equals(node.getNodeName())) {
			   if(node.getTextContent().contains("E")){
				   node.setTextContent( BigDecimal.valueOf(Double.parseDouble(node.getTextContent())).toPlainString());
			   }
		   }
		}

		//total element inside of start
		Node total = doc.createElement("total");
		doc.getDocumentElement().appendChild(total);
		total.setTextContent(""+scoresInfoList.getLength());

		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");

		//Write it to a String to return
		StreamResult result = new StreamResult(new StringWriter());
		DOMSource source = new DOMSource(doc);
		transformer.transform(source, result);

		String xmlString = result.getWriter().toString();

		return xmlString;
	}

	public static String getCharacterDataFromElement(Element e) {
	    Node child = e.getFirstChild();
	    if (child instanceof CharacterData) {
	       CharacterData cd = (CharacterData) child;
	       return cd.getData();
	    }
	    return "?";
	}

	/**
	 * 
	 * @param xml
	 * @return The Custom Xml String to send to Oracle.
	 * @throws ParserConfigurationException
	 * @throws TransformerException
	 * @throws SAXException
	 * @throws IOException
	 */
	public String createCustomXml(String xml, MemberInfo memberInfo, String custEventNm, 
			TagMetadata tagMetaData, String lyl_l_id, String topologyName) 
			throws ParserConfigurationException, TransformerException, SAXException, IOException{

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
 
		// root elements
		Document doc = docBuilder.newDocument();
		Element rootElement = doc.createElement("ns2:triggerCustomEvent");
		doc.appendChild(rootElement);

		rootElement.setAttribute("xmlns", "http://ws.services.responsys.com");
		rootElement.setAttribute("xmlns:ns2", "http://rest.ws.services.responsys.com");
		rootElement.setAttribute("priorityLevel", "2");

		// customEvent elements
		Element customEvent = doc.createElement("ns2:customEvent");
		rootElement.appendChild(customEvent);

		//eventName element inside of customEvent
		Element eventName = doc.createElement("eventName");
		if(custEventNm!=null && !custEventNm.equals(""))
				eventName.appendChild(doc.createTextNode(custEventNm));
			customEvent.appendChild(eventName);


		// recipientData elements
		Element recipientData = doc.createElement("ns2:recipientData");
		rootElement.appendChild(recipientData);

		//recipient element inside of recipientData
		Element recipient = doc.createElement("recipient");
		recipientData.appendChild(recipient);

		//listName element inside of recipient
		Element listName = doc.createElement("listName");
		recipient.appendChild(listName);

		//eventName element inside of listName
		Element folderName = doc.createElement("folderName");
		folderName.appendChild(doc.createTextNode("!MasterData"));
		listName.appendChild(folderName);

		//eventName element inside of listName
		Element objectName = doc.createElement("objectName");
		objectName.appendChild(doc.createTextNode("CONTACTS_LIST"));
		listName.appendChild(objectName);	

		//customerId element inside of recipient
		Element customerId = doc.createElement("customerId");
		customerId.appendChild(doc.createTextNode(memberInfo.getEid()));
		recipient.appendChild(customerId);

		//matchColumnName1 element inside of recipient
		Element matchColumnName1 = doc.createElement("matchColumnName1");
		matchColumnName1.appendChild(doc.createTextNode("CUSTOMER_ID_"));
		recipient.appendChild(matchColumnName1);

		//optionalData element inside of recipientData
		Element optionalData = doc.createElement("optionalData");
		recipientData.appendChild(optionalData);

		//name element inside of optionalData
		Element name = doc.createElement("name");
		//name.appendChild(doc.createTextNode("variable1"));
		name.appendChild(doc.createTextNode("occasion"));
		optionalData.appendChild(name);

		//value element inside of optionalData
		Element value = doc.createElement("value");
		optionalData.appendChild(value);
		//value.appendChild(doc.createCDATASection("RTS_DATA"));
		if(tagMetaData != null)
			value.appendChild(doc.createTextNode(tagMetaData.getPurchaseOccasion()));

		//Optional Data for adding the MDTag, BU and SUB_BU
		//optionalData element inside of recipientData
		if(!topologyName.equalsIgnoreCase(Constants.POS_PURCHASE)){
			Element optionalData2 = doc.createElement("optionalData");
			recipientData.appendChild(optionalData2);
			Element name2 = doc.createElement("name");
			name2.appendChild(doc.createTextNode("mdTag"));
			optionalData2.appendChild(name2);
			Element value2 = doc.createElement("value");
			optionalData2.appendChild(value2);
			if(tagMetaData!=null && tagMetaData.getMdTag()!=null && !tagMetaData.getMdTag().equals(""))
				value2.appendChild(doc.createTextNode(tagMetaData.getMdTag()));
		}
		
		if(topologyName.equalsIgnoreCase(Constants.POS_PURCHASE)){
			Element optionalData2 = doc.createElement("optionalData");
			recipientData.appendChild(optionalData2);
			Element name2 = doc.createElement("name");
			name2.appendChild(doc.createTextNode("item"));
			optionalData2.appendChild(name2);
			Element value2 = doc.createElement("value");
			optionalData2.appendChild(value2);
			if(tagMetaData!=null && tagMetaData.getDivLine()!=null && !tagMetaData.getDivLine().equals(""))
				value2.appendChild(doc.createTextNode(tagMetaData.getDivLine()));
		}

		Element optionalData3 = doc.createElement("optionalData");
		recipientData.appendChild(optionalData3);
		Element name3 = doc.createElement("name");
		name3.appendChild(doc.createTextNode("businessUnit"));
		optionalData3.appendChild(name3);
		Element value3 = doc.createElement("value");
		optionalData3.appendChild(value3);
		if(tagMetaData!=null && tagMetaData.getBusinessUnit()!=null && !tagMetaData.getBusinessUnit().equals(""))
			value3.appendChild(doc.createTextNode(tagMetaData.getBusinessUnit()));

		Element optionalData4 = doc.createElement("optionalData");
		recipientData.appendChild(optionalData4);
		Element name4 = doc.createElement("name");
		name4.appendChild(doc.createTextNode("subBusinessUnit"));
		optionalData4.appendChild(name4);
		Element value4 = doc.createElement("value");
		optionalData4.appendChild(value4);
		if(tagMetaData!=null && tagMetaData.getSubBusinessUnit()!=null && !tagMetaData.getSubBusinessUnit().equals(""))
			value4.appendChild(doc.createTextNode(tagMetaData.getSubBusinessUnit()));

		Element optionalData5 = doc.createElement("optionalData");
		recipientData.appendChild(optionalData5);
		Element name5 = doc.createElement("name");
		name5.appendChild(doc.createTextNode("MEMBERID"));
		optionalData5.appendChild(name5);
		Element value5 = doc.createElement("value");
		optionalData5.appendChild(value5);
		value5.appendChild(doc.createTextNode(lyl_l_id));
		
		Element optionalData6 = doc.createElement("optionalData");
		recipientData.appendChild(optionalData6);
		Element name6 = doc.createElement("name");
		name6.appendChild(doc.createTextNode("OPT_TYPE_SC"));
		optionalData6.appendChild(name6);
		Element value6 = doc.createElement("value");
		optionalData6.appendChild(value6);
		value6.appendChild(doc.createTextNode(memberInfo.getSrs_opt_in()!=null ? memberInfo.getSrs_opt_in() : "null"));
		
		Element optionalData7 = doc.createElement("optionalData");
		recipientData.appendChild(optionalData7);
		Element name7 = doc.createElement("name");
		name7.appendChild(doc.createTextNode("OPT_TYPE_KM"));
		optionalData7.appendChild(name7);
		Element value7 = doc.createElement("value");
		optionalData7.appendChild(value7);
		value7.appendChild(doc.createTextNode(memberInfo.getKmt_opt_in()!=null ? memberInfo.getKmt_opt_in() : "null"));
		
		Element optionalData8 = doc.createElement("optionalData");
		recipientData.appendChild(optionalData8);
		Element name8 = doc.createElement("name");
		name8.appendChild(doc.createTextNode("OPT_TYPE_LY"));
		optionalData8.appendChild(name8);
		Element value8 = doc.createElement("value");
		optionalData8.appendChild(value8);
		value8.appendChild(doc.createTextNode(memberInfo.getSyw_opt_in()!=null ? memberInfo.getSyw_opt_in() : "null"));

		//Generate the String from the xml document.
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");

		//Write it to a String to return
		StreamResult result = new StreamResult(new StringWriter());
		DOMSource source = new DOMSource(doc);
		transformer.transform(source, result);

		String xmlString = result.getWriter().toString();

		//Not an efficient way to perform this in this method but since Oracle wants new tags like element in place of scoresInfo
		//and scoresInfo to be upgraded as parent node with element nodes inside them. The requirement itself is very customized.
		/*String interminStr = xmlString.replace("RTS_DATA", "<RTS> " +xml+ " </RTS>");
		interminStr = interminStr.replace("<start>", "").replace("</start>", "");
		interminStr = interminStr.replace("scoresInfo", "element");
		String finalXmlStr = interminStr.substring(0, interminStr.indexOf("<element>"))+" <scoresInfo> "  
				+ interminStr.substring(interminStr.indexOf("<element>"),interminStr.lastIndexOf("</element>")+10)  
					+ " </scoresInfo> " + interminStr.substring(interminStr.lastIndexOf("</element>")+10,interminStr.length());*/
		String finalXmlStr = xmlString;
		LOGGER.info("customXml =  "+finalXmlStr);

		return finalXmlStr;
	}


	/**
	 * 
	 * @param s
	 * @return String without the Byte-Order-Map
	 */
	private static String removeUTF8BOM(String s) {
        if (s.startsWith(UTF8_BOM)) {
            s = s.substring(1);
        }
        return s;
    }

	public TagMetadata getTagMetaData(String tag) {
		TagMetadata tagMetaData = tagMetadataDao.getDetails(tag);
		return tagMetaData;
	}

	private TagMetadata getTagMetaDataInfo(org.json.JSONObject obj){
		TagMetadata tagMetaData = null;

		try {
			
			//If it gets here, it means that the Responsys is not ready with the Unknown Tags or there is no Unknown tag with % > 95
			if(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).has("mdTag") && ((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).has("occassion") &&
					((org.json.JSONObject)obj.getJSONArray("scoresInfo").get(0)).get("occassion").toString().equalsIgnoreCase("Unknown"))
				return null;
			
			tagMetaData = new TagMetadata();
			tagMetaData.setPurchaseOccassion(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("occassion")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("occassion").toString() : null);
			tagMetaData.setBusinessUnit(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("businessUnit")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("businessUnit").toString() : null);
			tagMetaData.setSubBusinessUnit(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("subBusinessUnit")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("subBusinessUnit").toString() : null);
			tagMetaData.setMdTag(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("mdTag")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("mdTag").toString() : null);
			tagMetaData.setFirst5CharMdTag(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("mdTag")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("mdTag").toString().substring(0,5) : null);

		} catch (org.json.JSONException e) {
			LOGGER.info(e.getMessage());
		}

		return tagMetaData;
	}


	/**
	 * 
	 * @param tagsMetaList
	 * @return List of Unknown tags that responsys is ready to process
	 */
	public HashSet<String> getReadyToProcessTags(){
		
		if(activeTags.size()==0)
			activeTags = tagResponsysActiveDao.getResponsysActiveTagsList();
		
		return activeTags;
	}

	/**
	 * 
	 * @param arr
	 * @param inputTags
	 * @return TagMetadata with the Unknown Winning Tag information 
	 */
	public TagMetadata determineUnknownWinner(org.json.JSONArray arr){
		TreeMap<Integer, TagMetadata> winnerMap = new TreeMap<Integer, TagMetadata>();
		TagMetadata winnerTag = null;

		try {
			
			//Hit the mongo only if it an unknown tag
			//readyToProcessTags = getReadyToProcessTags(inputTags);
			//getWinnerMap(readyToProcessTags, winnerMap, arr);
			getWinnerMap(activeTags, winnerMap, arr);
			

			//Check if the winning tags are all Unknown tags, pick the one with the percetile of 95%\
			if(winnerMap.size() > 0){
				Map.Entry<Integer, TagMetadata> entry = (Entry<Integer, TagMetadata>) winnerMap.entrySet().iterator().next();
				Integer winnerRank = entry.getKey();
				winnerTag = entry.getValue();

				swapJSONObjects(arr, winnerRank);
			}
		}catch (org.json.JSONException e) {
			e.printStackTrace();
			LOGGER.info("Error determining the winning Unknown tag to send to Responsys");
		}

		return winnerTag;
	}
	
	
	/**
	 * 
	 * @param tags
	 * @param winnerMap
	 * @param arr
	 * @throws JSONException
	 */
	private void getWinnerMap(HashSet<String> tags,
			TreeMap<Integer, TagMetadata> winnerMap, org.json.JSONArray arr)
			throws JSONException {
		
		TagMetadata tagMetaData = null;
		for(int i=0; i< arr.length() ; i++){
			Iterator<String> iter = tags.iterator();
			while(iter.hasNext()){
				String tag =  iter.next();
				if(((org.json.JSONObject)arr.get(i)).has("mdTag") && 
						((org.json.JSONObject)arr.get(i)).has("occassion") &&
						((org.json.JSONObject)arr.get(i)).get("mdTag").toString().substring(0, 5).equalsIgnoreCase(tag)){
					
					Integer rank = (Integer) ((org.json.JSONObject)arr.get(i)).get("rank");
					//Populate the TagMetaData Object from the API response
					tagMetaData = new TagMetadata();
					tagMetaData.setPurchaseOccassion(((org.json.JSONObject) arr.get(i)).get("occassion")!= null ? 
							((org.json.JSONObject) arr.get(i)).get("occassion").toString() : null);
					tagMetaData.setBusinessUnit(((org.json.JSONObject) arr.get(i)).get("businessUnit")!= null ? 
							((org.json.JSONObject) arr.get(i)).get("businessUnit").toString() : null);
					tagMetaData.setSubBusinessUnit(((org.json.JSONObject) arr.get(i)).get("subBusinessUnit")!= null ? 
							((org.json.JSONObject) arr.get(i)).get("subBusinessUnit").toString() : null);
					tagMetaData.setMdTag(((org.json.JSONObject) arr.get(i)).get("mdTag")!= null ? 
							((org.json.JSONObject) arr.get(i)).get("mdTag").toString() : null);
					tagMetaData.setFirst5CharMdTag(((org.json.JSONObject) arr.get(i)).get("mdTag")!= null ? 
							((org.json.JSONObject) arr.get(i)).get("mdTag").toString().substring(0,5) : null);
					tagMetaData.setPercentile((Double) ((org.json.JSONObject)arr.get(i)).getDouble("percentile"));
					
					winnerMap.put(rank, tagMetaData);
					return;
				
				}
			}
		}
		
	}


	/**
	 * Swap the JsonArray with the Winning Occassion/Tag
	 * @param arr
	 * @param winnerRank
	 * @throws org.json.JSONException
	 */
	private void swapJSONObjects(org.json.JSONArray arr, Integer winnerRank)
			throws org.json.JSONException {

		int swapIndex = winnerRank -1;

		org.json.JSONObject object = (org.json.JSONObject) arr.get(swapIndex);
		org.json.JSONObject object2 = (org.json.JSONObject) arr.get(0);

		object.remove("rank");
		object.put("rank", 1);

		object2.remove("rank");
		object2.put("rank", winnerRank);

		arr.put(swapIndex, object2);
		arr.put(0, object);
	}

	/**
	 * 
	 * @param tags
	 * @return List of TagMetadata information
	 */
	public ArrayList<TagMetadata> getTagMetaDataList(String tags) {
		ArrayList<TagMetadata> tagMetaDataList = tagMetadataDao.getDetailsList(tags);
		return tagMetaDataList;
	}

	/**
	 * Send The XML to Responsys
	 * @param xmlWithoutBOM
	 * @return the result of sending the XML to Responsys
	 */
	public StringBuffer sendToResponsys(String xmlWithoutBOM, MemberInfo memberInfo){
		HttpURLConnection connection = null;
		BufferedReader in = null;
		OutputStreamWriter out = null;
		StringBuffer strBuff = new StringBuffer();
		long time = System.currentTimeMillis();
		try {
			connection = HttpClientUtils.getConnectionWithBasicAuthentication(AuthPropertiesReader
					.getProperty(memberInfo.getWinningOptIn()+"WebserviceURL"),"application/xml", "POST",AuthPropertiesReader
					.getProperty(memberInfo.getWinningOptIn()+"Usrname"), AuthPropertiesReader
					.getProperty(memberInfo.getWinningOptIn()+"Password"));

			connection.setConnectTimeout(12000);
			connection.setReadTimeout(12000);
			
			out = new OutputStreamWriter(connection.getOutputStream());
			LOGGER.info(xmlWithoutBOM);
			out.write(xmlWithoutBOM);
			out.close();

			LOGGER.debug("After Creating outWriter");
			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			int c;
			while ((c = in.read()) != -1) {
				strBuff.append((char) c); 
			}
			LOGGER.info("time take to call Oracle = " + (System.currentTimeMillis() - time));
		}catch (java.net.SocketTimeoutException e1) {
		     LOGGER.error("PERSIST: Connection timed out in Oracle --- "+ memberInfo.getEid(), e1.getMessage() + "---" + memberInfo.getEid());
			 strBuff.setLength(0);
	    }catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("PERSIST: IOException occured in sendResponse "+ memberInfo.getEid(), e.getMessage() + "---" + memberInfo.getEid());
		}
		finally {
			try {
								
				if(out!=null) 
					out.close(); 
				if (in != null) 
					in.close();
				if (connection != null) 
					connection.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.error("Exception occured in getResponseServiceResult: finally: catch block ", e);
			}
		}
		return strBuff;

	}
	
	/**
	 * Method to check whether vibes is ready with the BU/SubBU
	 * @param occasion
	 * @param bussUnit
	 * @param custVibesEvent
	 * @return true or false based on whether Vibes is ready to accept the Bu/SubBu
	 */
	public boolean isVibesActiveWithEvent(String occasion, String bussUnit, StringBuilder custVibesEvent){
		HashMap<String, HashMap<String, String>> eventVibesActiveMap = eventsVibesActiveDao.getVibesActiveEventsList();
		if(eventVibesActiveMap.get(occasion)!= null){
			if(eventVibesActiveMap.get(occasion).get(bussUnit)!=null)
				custVibesEvent.append(eventVibesActiveMap.get(occasion).get(bussUnit));
			else
				custVibesEvent.append(eventVibesActiveMap.get(occasion).get(null));
		}
		
		//Log the info incase Vibes isn;t ready with the occasion and BU
		if(custVibesEvent.toString().isEmpty())
			LOGGER.info("Vibes is not ready for Occasion "+occasion+ " for BU "+bussUnit);
		
		return (!custVibesEvent.toString().isEmpty());
	}
	
	/**
	 * 
	 * @param objToSend
	 * @return  TagMetaData
	 * @throws JSONException
	 */
	public TagMetadata getTagMetadata(org.json.JSONObject objToSend)
			throws JSONException {
		//get tagMetadata information and set the mdTag with zeros
		TagMetadata tagMetadata = null;
		String tag = tagModelsMap.get(Integer.parseInt((String) objToSend.get("modelId")));
		tagMetadata = tagMetadataDao.getBuSubBu(tag);
		tagMetadata.setMdTag(tag+"8000000000000");
		return tagMetadata;
	}
	

	public TagMetadata getTagMetadata(TagMetadata tagMetadata,String divLine)
			throws JSONException {
		
		//Commenting this code just for the holiday season to suppress emails..
		//tagMetadata = divLineBuSubDao.getBuSubBu(tagMetadata,divLine);
		
		tagMetadata = divLineBuSubDao.getBuSubBuHolidaySeason(tagMetadata,divLine);

		return tagMetadata;
	}
}