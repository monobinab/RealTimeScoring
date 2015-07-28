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
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.MemberMDTags2Dao;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.OccasionResponsesDao;
import analytics.util.dao.OccationCustomeEventDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.dao.TagResponsysActiveDao;
import analytics.util.dao.TagVariableDao;
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
	//private Map<String, String> activeTagMap;
	private Map<Integer, String> tagModelsMap;
	private static HashSet<String> activeTags = new HashSet<String>();
	private DivLineBuSubDao divLineBuSubDao;
	

	private static final String UTF8_BOM = "\uFEFF";
	private TagResponsysActiveDao tagResponsysActiveDao;
	private static final String validUnownTags = "Top 5% of MSM,Unknown";
	
	private EventsVibesActiveDao eventsVibesActiveDao;
	HashMap<String, HashMap<String, String>> eventVibesActiveMap = new HashMap<String, HashMap<String, String>>();
	
	private MemberMDTags2Dao memberMDTags2Dao;
	private MemberMDTagsDao memberMDTagsDao;
	
	//ArrayList<TagMetadata> metaDataList = new ArrayList<TagMetadata>();
	//ArrayList<String> readyTags = new ArrayList<String>();

	public ResponsysUtil() {
		//
		
		tagMetadataDao = new TagMetadataDao();
		
		occationCustomeEventDao = new OccationCustomeEventDao();
		occasionResponsesDao = new OccasionResponsesDao();
		tagResponsysActiveDao =  new TagResponsysActiveDao();
		tagVariableDao = new TagVariableDao();
		eventsVibesActiveDao = new EventsVibesActiveDao();
		eventVibesActiveMap = eventsVibesActiveDao.getVibesActiveEventsList();
		memberInfoDao = new MemberInfoDao();
		tagVariableDao = new TagVariableDao();
		//tagResponsysActiveDao =  new TagResponsysActiveDao();
		//activeTagMap = tagResponsysActiveDao.getResponsysActiveTagsList();
		activeTags = getReadyToProcessTags();
		//activeTags.addAll(activeTagMap.keySet());
		tagModelsMap = tagVariableDao.getTagModelIds(activeTags);
		divLineBuSubDao = new DivLineBuSubDao();
		memberMDTagsDao = new MemberMDTagsDao();
		memberMDTags2Dao = new MemberMDTags2Dao();
 
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
	public String callRtsAPI(String lyl_l_id) {
		String baseURL = Constants.SCORING_API_PRE+lyl_l_id+Constants.SCORING_API_POST;
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
			//LOGGER.info(jsonRespString);	

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
		StringBuffer strBuff = new StringBuffer();
		TagMetadata winningTag = null;
		try {

			//Only for Testing purpose
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Duress</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>         <customerId>123456</customerId>         <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <modelId>15</modelId>               <rank>1</rank>               <scorePercentile>0.9478713429</scorePercentile>               <percentile>66</percentile>               <totalScore>0.0406676691</totalScore>               <category>Mens Apparel</category>               <occasion>moving</occasion>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.0050142</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>2</rank>               <scorePercentile>0.8913420857</scorePercentile>               <percentile>62</percentile>               <totalScore>0.0408448841</totalScore>               <category>Footwear</category>               <occasion>moving</occasion>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0056278</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>5</rank>               <scorePercentile>0.5012802</scorePercentile>               <percentile>35</percentile>               <totalScore>0.0044008155</totalScore>               <category>Consumer Electronics</category>               <occasion>moving</occasion>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0012802</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>6</rank>               <scorePercentile>0.5010286</scorePercentile>               <percentile>35</percentile>               <totalScore>0.0098548109</totalScore>               <category>Sporting Goods</category>               <occasion>moving</occasion>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0010286</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>8</rank>               <scorePercentile>0.0228418143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0120221761</totalScore>               <category>Food &amp; Consumables</category>               <occasion>moving</occasion>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.0085561</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>9</rank>               <scorePercentile>0.0188801143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.035300613</totalScore>               <category>Hard Home</category>               <occasion>moving</occasion>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0045944</score>               <format>Kmart</format>             </element>             <element>               <modelId>64</modelId>               <rank>10</rank>               <scorePercentile>0.0182040143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0203054143</totalScore>               <category>Auto-DIY Tools Auto</category>               <occasion>moving</occasion>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.0039183</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>11</rank>               <scorePercentile>0.0179993143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0285055936</totalScore>               <category>Womens Apparel</category>               <occasion>moving</occasion>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.0037136</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>12</rank>               <scorePercentile>0.0176029143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0279726207</totalScore>               <category>Home Fashions</category>               <occasion>moving</occasion>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0033172</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>13</rank>               <scorePercentile>0.0156809143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0085896883</totalScore>               <category>Toys</category>               <occasion>moving</occasion>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0013952</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <scorePercentile>0.0155123143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0120416549</totalScore>               <category>Kids Apparel</category>               <occasion>moving</occasion>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0012266</score>               <format>Kmart</format>             </element>             <scoresInfo>               <lastUpdated>2015-03-06 19:26:01.0</lastUpdated>               <memberId>7081265362051654</memberId>               <total>11</total>             </RTS>]]>           </value>         </optionalData>         <optionalData>           <name>mdTag</name>           <value>LGSWS22160093010</value>         </optionalData>         <optionalData>           <name>subBusinessUnit</name>           <value>Sears Snowblower</value>         </optionalData>         <optionalData>           <name>businessUnit</name>           <value>Lawn And Garden</value>         </optionalData>       </ns2:recipientData>     </ns2:triggerCustomEvent>  ";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Moving</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo> <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Moving</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>              <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>           <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent> ";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Duress</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Duress</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Remodeling</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Remodeling</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>  ";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Replacement</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Replacement</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>  ";
	/*		  String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Unknown</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>999988887777</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Unknown</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>  ";
			  String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Purchase</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>         <customerId>133245393</customerId>         <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>          <name>subBusinessUnit</name>           <value>Sears Snowblower,Refrigerator</value>         </optionalData>         <optionalData>           <name>businessUnit</name>           <value>Lawn And Garden,Sears Appliance</value>         </optionalData>         <optionalData>           <name>MEMBERID</name>           <value>7081327008588950</value>         </optionalData>         <optionalData>           <name>Item</name>           <value>00112033</value>         </optionalData>   </ns2:recipientData>     </ns2:triggerCustomEvent> ";
			String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Purchase</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>133245393</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Snowblower,Refrigerator</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Lawn And Garden,Sears Appliance</value>     </optionalData>     <optionalData>       <name>MEMBERID</name>       <value>7081327008588950</value>     </optionalData>     <optionalData>       <name>Item</name>       <value>00112033</value>     </optionalData>     <optionalData>       <name>OPT_TYPE_LY</name>       <value>Y</value>     </optionalData>     <optionalData>       <name>OPT_TYPE_SC</name>       <value>Y</value>     </optionalData>     <optionalData>       <name>OPT_TYPE_KM</name>       <value>Y</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent> ";
			System.out.println(xml);
			String xmlWithoutBOM = removeUTF8BOM(xml);
			sendToResponsys(xmlWithoutBOM);
			if(true) 
				return null;
			*/
			
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
				
				winningTag.setEmailOptIn( memberInfo != null?memberInfo.getEmailOptIn():null);

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

			//retrieve the properties from responsys object
			String eid = null;
			TagMetadata tagMetadata = null;
			String lyl_l_id = null;
			org.json.JSONObject o = null;
			String customEventName = null;
			String l_id = null;
			String topologyName = null;

			if(responsysObj.getEid() != null)
				 eid = responsysObj.getEid();
			if(responsysObj.getTagMetadata() != null)
				tagMetadata = responsysObj.getTagMetadata();
			if(responsysObj.getLyl_id_no() != null)
				lyl_l_id = responsysObj.getLyl_id_no();
			if(responsysObj.getJsonObj() != null)
				o = responsysObj.getJsonObj();
			if(responsysObj.getL_id() != null)
				l_id = responsysObj.getL_id();
			if(responsysObj.getCustomEventName() != null)
				customEventName = responsysObj.getCustomEventName();
			if(responsysObj.getTopologyName() != null)
				topologyName = responsysObj.getTopologyName();

			processMessage(responsysObj.getMemberInfo(), customEventName, tagMetadata, lyl_l_id, topologyName, l_id);

			responsysObj = null;
			tagMetadata = null;
			o = null;

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
	/*public void processMessage(JSONObject obj, String eid, String customEventName, 
			TagMetadata tagMetadata, String lyl_l_id, String topologyName, String l_id) throws Exception{
		
		String json2XmlString = org.json.XML.toString(obj);
		//Adding the start tag(root tag) to make the xml valid so we can parse it.
		json2XmlString="<start>"+json2XmlString+"</start>";
	
		//Convert Exponential values to Plain text in the XML
		String xmlWithoutExpo = removeExponentialFromXml(json2XmlString);
	
		//Generate the Custome Xml to be sent to Oracle
		String customXml = createCustomXml(xmlWithoutExpo, eid, customEventName, tagMetadata, lyl_l_id);
	
		//BOM = Byte-Order-Mark
		//Remove the BOM to make the XML valid
		String xmlWithoutBOM = removeUTF8BOM(customXml);
	
		//System.out.println("customXml = " + customXml);
		StringBuffer strBuff = sendToResponsys(xmlWithoutBOM);
		//StringBuffer strBuff = new StringBuffer();
		
		//Persist info to Mongo after successfully transmission of message to Oracle.
		occasionResponsesDao.addOccasionResponse(l_id, eid, customEventName, !topologyName.equalsIgnoreCase("unknownOccasions")?tagMetadata.getPurchaseOccasion():"Unknown", tagMetadata.getBusinessUnit(), tagMetadata.getSubBusinessUnit(), 
				strBuff.toString().contains("<success>true</success>") ? "Y" : "N", tagMetadata.getMdTags(), topologyName);
	
		LOGGER.info("PERSIST: Winning Tag for Lid: " + lyl_l_id +" : "+tagMetadata.getMdTags());
		
		nullifyObjects(xmlWithoutBOM, xmlWithoutExpo, json2XmlString, customXml);
		
		System.out.println("Response String ====>" + strBuff.toString());
		strBuff = null;
		
		//return strBuff;
	}*/
	
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
	
		//System.out.println("customXml = " + customXml);
		StringBuffer strBuff = sendToResponsys(xmlWithoutBOM,memberInfo);
		//StringBuffer strBuff = new StringBuffer();
		
		//Persist info to Mongo after successfully transmission of message to Oracle.
		occasionResponsesDao.addOccasionResponse(l_id, memberInfo.getEid(), customEventName, 
				!topologyName.equalsIgnoreCase("unknownOccasions")?tagMetadata.getPurchaseOccasion():"Unknown", 
						tagMetadata, strBuff.toString().contains("<success>true</success>") ? "Y" : "N", topologyName);
	
		LOGGER.info("PERSIST: Winning Tag for Lid: " + lyl_l_id +" : "+tagMetadata.getMdTags());
		
		nullifyObjects(xmlWithoutBOM, customXml);
		
		System.out.println("Response String ====>" + strBuff.toString());
		strBuff = null;
		
		LOGGER.info("Time taken in Process Message = " + (System.currentTimeMillis() - time));
		//return strBuff;
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
			if(tagMetaData!=null && tagMetaData.getMdTags()!=null && !tagMetaData.getMdTags().equals(""))
				value2.appendChild(doc.createTextNode(tagMetaData.getMdTags()));
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
			tagMetaData.setMdTags(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("mdTag")!= null ? 
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
					tagMetaData.setMdTags(((org.json.JSONObject) arr.get(i)).get("mdTag")!= null ? 
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
					.getProperty(Constants.RESP_URL),"application/xml", "POST",AuthPropertiesReader
					.getProperty(memberInfo.getWinningOptIn()+"Usrname"), AuthPropertiesReader
					.getProperty(memberInfo.getWinningOptIn()+"Password"));

			connection.setConnectTimeout(12000);
			connection.setReadTimeout(12000);
						   
			out = new OutputStreamWriter(connection.getOutputStream());
			System.out.println(xmlWithoutBOM);
			out.write(xmlWithoutBOM);
			out.close();

			LOGGER.debug("After Creating outWriter");
			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			int c;
			while ((c = in.read()) != -1) {
				strBuff.append((char) c); 
			}
			System.out.println("time take to call Oracle = " + (System.currentTimeMillis() - time));
		//	System.out.println("Response String ====>" + strBuff.toString());
		}catch (java.net.SocketTimeoutException e1) {
		     LOGGER.error("PERSIST: Connection timed out in Oracle ", e1.getMessage() + "---" + memberInfo.getEid());
	    }catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("PERSIST: IOException occured in sendResponse ", e.getMessage() + "---" + memberInfo.getEid());
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

	public void nullifyObjects(String xmlWithoutBOM, String customXml ){
		xmlWithoutBOM = null;
		customXml = null;
	}

	
	/**
	 * Method to check whether vibes is ready with the BU/SubBU
	 * @param occasion
	 * @param bussUnit
	 * @param custVibesEvent
	 * @return true or false based on whether Vibes is ready to accept the Bu/SubBu
	 */
	public boolean isVibesActiveWithEvent(String occasion, String bussUnit, StringBuilder custVibesEvent){
		
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
		tagMetadata.setMdTags(tag+"8000000000000");
		return tagMetadata;
	}
	

	public TagMetadata getTagMetadata(TagMetadata tagMetadata,String divLine)
			throws JSONException {

		tagMetadata = divLineBuSubDao.getBuSubBu(tagMetadata,divLine);

		return tagMetadata;
	}
}