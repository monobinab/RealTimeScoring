package analytics.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.HttpClientUtils;
import analytics.util.SecurityUtils;
import analytics.util.dao.MemberInfoDao;
import analytics.util.dao.OccasionResponsesDao;
import analytics.util.dao.OccationCustomeEventDao;
import analytics.util.dao.TagMetadataDao;
import analytics.util.objects.TagMetadata;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ResponseBolt extends EnvironmentBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ResponseBolt.class);
	private MultiCountMetric countMetric;
	private OutputCollector outputCollector;
	private static final String UTF8_BOM = "\uFEFF";
	private MemberInfoDao memberInfoDao;
	private String host;
	private int port;
	private JedisPool jedisPool;
	private TagMetadataDao tagMetadataDao;
	private OccationCustomeEventDao occationCustomeEventDao;
	private OccasionResponsesDao occasionResponsesDao;
	
	public ResponseBolt(String systemProperty, String host, int port) {
		super(systemProperty);
		this.host = host;
		this.port = port;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		initMetrics(context);
		memberInfoDao = new MemberInfoDao();
		tagMetadataDao = new TagMetadataDao();
		occationCustomeEventDao = new OccationCustomeEventDao();
		occasionResponsesDao = new OccasionResponsesDao();
		this.outputCollector = collector;
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(100);
        jedisPool = new JedisPool(poolConfig,host, port, 100);
	}
	void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }


	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		String lyl_id_no = null; 
		
		try {
			
			if(input != null && input.contains("lyl_id_no")){
				lyl_id_no = input.getString(0);
				String scoreInfoJsonString = callRtsAPI(lyl_id_no);
				String l_id = SecurityUtils.hashLoyaltyId(lyl_id_no);
				
				//4-2-2015.Recent update to send responses only for 1 tag irrespective of 
				//how many tags we receive in the difference. This occasion tag 
				//for which the response has to be sent is taken from the 1st ranks occasion tags from the API call
				
				//Get the Difference Tags from Redis for an lid
				Jedis jedis = jedisPool.getResource();
				//Add Date as part of key so incase the Tags are not scored for the member
				//atleast we know we have to cleanup from Redis...			
				Date dNow = new Date( );
				SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd");
				String lId_Date = l_id +"~~~"+ft.format(dNow);
				
				String diffTags =  jedis.get("Responses:"+lId_Date).toString() ;
				jedisPool.returnResource(jedis);
				
				if(diffTags!=null && !"".equals(diffTags)){
					String[] tags = diffTags.split(",");
					//Send response for every new tag scored
					//length -1 because the last element would be the datestring set in the parsing bolt.
					for(int i=0 ;i<tags.length ;i++){
						String tag = tags[i];
						getResponseServiceResult(scoreInfoJsonString,lyl_id_no,tag);
						countMetric.scope("responses").incr();
					}
				}
				
				/*getResponseServiceResult(scoreInfoJsonString,lyl_id_no);
				countMetric.scope("responses").incr();*/

				/*//Delete the lid from redis after processing
				jedis = jedisPool.getResource();
				jedis.del("Responses:"+lId_Date);
				jedisPool.returnResource(jedis);*/
			}
			outputCollector.ack(input);
			
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("responses_failed").incr();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * Invokes the web intelligence web service that returns a token identifier and a status code in
	 * the response.
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
			LOGGER.debug("WI Response String: " + responseString);
			InputStream instream = response.getEntity().getContent();
			jsonRespString = read(instream);
			LOGGER.info(jsonRespString);	
			
		} catch (IOException e3) {
			e3.printStackTrace();
			LOGGER.error("IO Exception Occured " + baseURL + "\n" + e3);
			return null;
		} catch (Exception e5) {
			e5.printStackTrace();
			LOGGER.error("Error occured while calling the web service " + e5);
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
	 * @return
	 * @throws Exception
	 */
	public String getResponseServiceResult(String input, String lyl_l_id, String tag) throws Exception {
		LOGGER.info(" Testing - Entering the getResponseServiceResult method");
		StringBuffer strBuff = new StringBuffer();
		BufferedReader in = null;
		OutputStreamWriter out = null;
		HttpURLConnection connection = null;
		try {

			//Only for Testing purpose
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Duress</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>         <customerId>123456</customerId>         <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <modelId>15</modelId>               <rank>1</rank>               <scorePercentile>0.9478713429</scorePercentile>               <percentile>66</percentile>               <totalScore>0.0406676691</totalScore>               <category>Mens Apparel</category>               <occasion>moving</occasion>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.0050142</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>2</rank>               <scorePercentile>0.8913420857</scorePercentile>               <percentile>62</percentile>               <totalScore>0.0408448841</totalScore>               <category>Footwear</category>               <occasion>moving</occasion>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0056278</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>5</rank>               <scorePercentile>0.5012802</scorePercentile>               <percentile>35</percentile>               <totalScore>0.0044008155</totalScore>               <category>Consumer Electronics</category>               <occasion>moving</occasion>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0012802</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>6</rank>               <scorePercentile>0.5010286</scorePercentile>               <percentile>35</percentile>               <totalScore>0.0098548109</totalScore>               <category>Sporting Goods</category>               <occasion>moving</occasion>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0010286</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>8</rank>               <scorePercentile>0.0228418143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0120221761</totalScore>               <category>Food &amp; Consumables</category>               <occasion>moving</occasion>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.0085561</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>9</rank>               <scorePercentile>0.0188801143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.035300613</totalScore>               <category>Hard Home</category>               <occasion>moving</occasion>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0045944</score>               <format>Kmart</format>             </element>             <element>               <modelId>64</modelId>               <rank>10</rank>               <scorePercentile>0.0182040143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0203054143</totalScore>               <category>Auto-DIY Tools Auto</category>               <occasion>moving</occasion>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.0039183</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>11</rank>               <scorePercentile>0.0179993143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0285055936</totalScore>               <category>Womens Apparel</category>               <occasion>moving</occasion>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.0037136</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>12</rank>               <scorePercentile>0.0176029143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0279726207</totalScore>               <category>Home Fashions</category>               <occasion>moving</occasion>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0033172</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>13</rank>               <scorePercentile>0.0156809143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0085896883</totalScore>               <category>Toys</category>               <occasion>moving</occasion>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0013952</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <scorePercentile>0.0155123143</scorePercentile>               <percentile>1</percentile>               <totalScore>0.0120416549</totalScore>               <category>Kids Apparel</category>               <occasion>moving</occasion>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0012266</score>               <format>Kmart</format>             </element>             <scoresInfo>               <lastUpdated>2015-03-06 19:26:01.0</lastUpdated>               <memberId>7081265362051654</memberId>               <total>11</total>             </RTS>]]>           </value>         </optionalData>         <optionalData>           <name>mdTag</name>           <value>LGSWS22160093010</value>         </optionalData>         <optionalData>           <name>subBusinessUnit</name>           <value>Sears Snowblower</value>         </optionalData>         <optionalData>           <name>businessUnit</name>           <value>Lawn And Garden</value>         </optionalData>       </ns2:recipientData>     </ns2:triggerCustomEvent>  ";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Moving</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo> <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Moving</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>              <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent> ";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Duress</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Duress</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Remodeling</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Remodeling</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>  ";
			//String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Replacement</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>248143645</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Replacement</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>  ";
	/*		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?> <ns2:triggerCustomEvent xmlns:ns2=\"http://rest.ws.services.responsys.com\" xmlns=\"http://ws.services.responsys.com\" priorityLevel=\"2\">   <ns2:customEvent>     <eventName>RTS_Unknown</eventName>   </ns2:customEvent>   <ns2:recipientData>     <recipient>       <listName>         <folderName>!MasterData</folderName>         <objectName>CONTACTS_LIST_TEST</objectName>       </listName>       <customerId>999988887777</customerId>       <matchColumnName1>CUSTOMER_ID_</matchColumnName1>     </recipient>     <optionalData>       <name>variable1</name>       <value>         <![CDATA[<RTS>         <?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>           <statusCode>200</statusCode>           <status>success</status>           <scoresInfo>             <element>               <percentile>100</percentile>               <tag>0100</tag>               <score>0.0186600515</score>               <format>Sears</format>               <modelId>35</modelId>               <rank>1</rank>               <category>Cooking Appliance Model</category>               <mdTag>HACKS2103005008</mdTag>               <businessUnit>Home Appliance</businessUnit>               <modelName>S_SCR_HA_COOK</modelName>               <style>Gas</style>               <color>White</color>               <occassion>Unknown</occassion>               <subBusinessUnit>Sears Cooktop</subBusinessUnit>               <brand>Kenmore</brand>               <scoreDate>2015-03-07</scoreDate>             </element>             <element>               <modelId>64</modelId>               <rank>2</rank>               <percentile>99</percentile>               <category>Auto-DIY Tools Auto</category>               <modelName>K_SCR_DIY_TOOL_AUTO</modelName>               <tag>2139</tag>               <score>0.1211878</score>               <format>Kmart</format>             </element>             <element>               <modelId>8</modelId>               <rank>3</rank>               <percentile>100</percentile>               <category>Sporting Goods</category>               <modelName>K_SCR_FSG</modelName>               <tag>2138</tag>               <score>0.0856989</score>               <format>Kmart</format>             </element>             <element>               <modelId>2</modelId>               <rank>4</rank>               <percentile>99</percentile>               <category>Consumer Electronics</category>               <modelName>K_SCR_CE</modelName>               <tag>2128</tag>               <score>0.0920899</score>               <format>Kmart</format>             </element>             <element>               <modelId>14</modelId>               <rank>5</rank>               <percentile>100</percentile>               <category>Lawn &amp; Garden</category>               <modelName>K_SCR_LG</modelName>               <tag>2133</tag>               <score>0.0332487</score>               <format>Kmart</format>             </element>             <element>               <modelId>19</modelId>               <rank>6</rank>               <percentile>94</percentile>               <category>Womens Apparel</category>               <modelName>K_SCR_WAPP</modelName>               <tag>2127</tag>               <score>0.1139413</score>               <format>Kmart</format>             </element>             <element>               <modelId>16</modelId>               <rank>7</rank>               <percentile>100</percentile>               <category>Outdoor Living</category>               <modelName>K_SCR_ODL</modelName>               <tag>2134</tag>               <score>0.0189727</score>               <format>Kmart</format>             </element>             <element>               <modelId>6</modelId>               <rank>1</rank>               <percentile>100</percentile>               <category>Food &amp; Consumables</category>               <modelName>K_SCR_FOOD_CONS</modelName>               <tag>2124</tag>               <score>0.720714</score>               <format>Kmart</format>             </element>             <element>               <modelId>18</modelId>               <rank>9</rank>               <percentile>95</percentile>               <category>Toys</category>               <modelName>K_SCR_TOY</modelName>               <tag>2140</tag>               <score>0.0793629</score>               <format>Kmart</format>             </element>             <element>               <modelId>7</modelId>               <rank>10</rank>               <percentile>95</percentile>               <category>Footwear</category>               <modelName>K_SCR_FOOTWEAR</modelName>               <tag>2129</tag>               <score>0.0722123</score>               <format>Kmart</format>             </element>             <element>               <modelId>12</modelId>               <rank>11</rank>               <percentile>94</percentile>               <category>Home Fashions</category>               <modelName>K_SCR_HF</modelName>               <tag>2137</tag>               <score>0.0555195</score>               <format>Kmart</format>             </element>             <element>               <modelId>1</modelId>               <rank>12</rank>               <percentile>97</percentile>               <category>Auto</category>               <modelName>K_SCR_AUTO</modelName>               <tag>2126</tag>               <score>0.0024181</score>               <format>Kmart</format>             </element>             <element>               <modelId>15</modelId>               <rank>13</rank>               <percentile>94</percentile>               <category>Mens Apparel</category>               <modelName>K_SCR_MAPP</modelName>               <tag>2125</tag>               <score>0.04336</score>               <format>Kmart</format>             </element>             <element>               <modelId>13</modelId>               <rank>14</rank>               <percentile>93</percentile>               <category>Kids Apparel</category>               <modelName>K_SCR_KAPP</modelName>               <tag>2123</tag>               <score>0.0399338</score>               <format>Kmart</format>             </element>             <element>               <modelId>10</modelId>               <rank>15</rank>               <percentile>92</percentile>               <category>Hard Home</category>               <modelName>K_SCR_HARD_HOME</modelName>               <tag>2131</tag>               <score>0.0341598</score>               <format>Kmart</format>             </element>             <element>               <modelId>5</modelId>               <rank>16</rank>               <percentile>91</percentile>               <category>Fine Jewelry</category>               <modelName>K_SCR_FNJL</modelName>               <tag>2132</tag>               <score>0.0106667</score>               <format>Kmart</format>             </element>             <element>               <modelId>36</modelId>               <rank>17</rank>               <percentile>75</percentile>               <category>Dishwasher Model</category>               <modelName>S_SCR_HA_DISH</modelName>               <tag>0101</tag>               <score>0.0006319</score>               <format>Sears</format>             </element>             <element>               <modelId>44</modelId>               <rank>18</rank>               <percentile>60</percentile>               <category>Kids apparel</category>               <modelName>S_SCR_KAPP</modelName>               <tag>0122</tag>               <score>0.0041874</score>               <format>Sears</format>             </element>             <element>               <modelId>31</modelId>               <rank>19</rank>               <percentile>57</percentile>               <category>Fitness &amp; Sporting Goods</category>               <modelName>S_SCR_FITNESS</modelName>               <tag>0242</tag>               <score>0.0005996</score>               <format>Sears</format>             </element>             <element>               <modelId>25</modelId>               <rank>20</rank>               <percentile>54</percentile>               <category>Automotive</category>               <modelName>S_SCR_AUTO</modelName>               <tag>0125</tag>               <score>0.0015376</score>               <format>Sears</format>             </element>             <element>               <modelId>60</modelId>               <rank>21</rank>               <percentile>53</percentile>               <category>Toys</category>               <modelName>S_SCR_TOYS</modelName>               <tag>0141</tag>               <score>0.0001329</score>               <format>Sears</format>             </element>             <element>               <modelId>33</modelId>               <rank>22</rank>               <percentile>52</percentile>               <category>Footwear</category>               <modelName>S_SCR_FOOTWEAR</modelName>               <tag>0129</tag>               <score>0.0050354</score>               <format>Sears</format>             </element>             <element>               <modelId>34</modelId>               <rank>23</rank>               <percentile>52</percentile>               <category>Home Appliance</category>               <modelName>S_SCR_HA_ALL</modelName>               <tag>0208</tag>               <score>0.00363</score>               <format>Sears</format>             </element>             <element>               <modelId>63</modelId>               <rank>24</rank>               <percentile>51</percentile>               <category>Washer/Dryer Model</category>               <modelName>S_SCR_WASH_DRY</modelName>               <tag>0099</tag>               <score>0.0008536</score>               <format>Sears</format>             </element>             <element>               <modelId>55</modelId>               <rank>25</rank>               <percentile>51</percentile>               <category>Outdoor Living</category>               <modelName>S_SCR_ODL</modelName>               <tag>0136</tag>               <score>0.0003893</score>               <format>Sears</format>             </element>             <element>               <modelId>27</modelId>               <rank>26</rank>               <percentile>44</percentile>               <category>Consumer Electronics</category>               <modelName>S_SCR_CE</modelName>               <tag>0127</tag>               <score>0.0006115</score>               <format>Sears</format>             </element>             <element>               <modelId>59</modelId>               <rank>27</rank>               <percentile>38</percentile>               <category>Tools</category>               <modelName>S_SCR_TOOLS</modelName>               <tag>0143</tag>               <score>0.002867</score>               <format>Sears</format>             </element>             <element>               <modelId>43</modelId>               <rank>28</rank>               <percentile>26</percentile>               <category>All Home</category>               <modelName>S_SCR_HOME</modelName>               <tag>0131</tag>               <score>0.0022796</score>               <format>Sears</format>             </element>             <element>               <modelId>47</modelId>               <rank>29</rank>               <percentile>24</percentile>               <category>Lawn &amp; Garden</category>               <modelName>S_SCR_LG_ADAS</modelName>               <tag>0135</tag>               <score>0.0007296</score>               <format>Sears</format>             </element>             <element>               <modelId>32</modelId>               <rank>30</rank>               <percentile>24</percentile>               <category>Fine Jewelry</category>               <modelName>S_SCR_FNJL</modelName>               <tag>0134</tag>               <score>0.0003573</score>               <format>Sears</format>             </element>             <element>               <modelId>62</modelId>               <rank>31</rank>               <percentile>22</percentile>               <category>Womens Apparel</category>               <modelName>S_SCR_WAPP</modelName>               <tag>0126</tag>               <score>0.0035903</score>               <format>Sears</format>             </element>             <element>               <modelId>57</modelId>               <rank>32</rank>               <percentile>22</percentile>               <category>Refrigerator Model</category>               <modelName>S_SCR_REGRIG</modelName>               <tag>0102</tag>               <score>0.000302</score>               <format>Sears</format>             </element>             <element>               <modelId>52</modelId>               <rank>33</rank>               <percentile>1</percentile>               <category>Mens apparel</category>               <modelName>S_SCR_MAPP</modelName>               <tag>0124</tag>               <score>0.0033073</score>               <format>Sears</format>             </element>           </scoresInfo>           <lastUpdated>2015-03-16 14:24:07.873</lastUpdated>           <memberId>7081057588230760</memberId>           <total>33</total>         </RTS>]]>       </value>     </optionalData>     <optionalData>       <name>mdTag</name>       <value>HACKS2103005008</value>     </optionalData>     <optionalData>       <name>businessUnit</name>       <value>Home Appliance</value>     </optionalData>     <optionalData>       <name>subBusinessUnit</name>       <value>Sears Cooktop</value>     </optionalData>   </ns2:recipientData> </ns2:triggerCustomEvent>  ";
			System.out.println(xml);
			String xmlWithoutBOM = removeUTF8BOM(xml);
			out.write(xmlWithoutBOM);
			out.close();*/
			
			org.json.JSONObject o = new org.json.JSONObject(input);
			
			//Get the necessary variables for populating in the response xml
			String l_id = SecurityUtils.hashLoyaltyId(lyl_l_id);
			String eid = memberInfoDao.getMemberInfoEId(l_id);
			TagMetadata tagMetaData = getTagMetaData(tag);
			String custEventName = occationCustomeEventDao.getCustomeEventName(tagMetaData.getPurchaseOccasion());
			

			//4-15-2015. Check if the Tag is among the top 5 mdtags from the API Call.
			//Send the XML to responses only when the input tag is among the top 5.
			if(!isMdTagPresentAmongTop5TagsFromAPI(o,tag)){
				occasionResponsesDao.addOccasionResponse(l_id, eid, custEventName, tagMetaData.getPurchaseOccasion(), tagMetaData.getBusinessUnit(), tagMetaData.getSubBusinessUnit(), 
						"N/A", tag);
				LOGGER.info("Not Sending the Tag " + tag + " to Responsys");
				tagMetaData = null;
				o = null;
				return strBuff.toString();
			}
			
			String json2XmlString = org.json.XML.toString(o);
			//Adding the start tag(root tag) to make the xml valid so we can parse it.
			json2XmlString="<start>"+json2XmlString+"</start>";
			
			connection = HttpClientUtils.getConnectionWithBasicAuthentication(AuthPropertiesReader
					.getProperty(Constants.RESP_URL),"application/xml", "POST",AuthPropertiesReader
					.getProperty(Constants.RESP_URL_USER_NAME), AuthPropertiesReader
					.getProperty(Constants.RESP_URL_PASSWORD));
			
			out = new OutputStreamWriter(connection.getOutputStream());
			LOGGER.debug("After Creating outWriter");
			
			//Convert Exponential values to Plain text in the XML
			String xmlWithoutExpo = removeExponentialFromXml(json2XmlString);
			
			//Generate the Custome Xml to be sent to Oracle
			String customXml = createCustomXml(xmlWithoutExpo,eid,custEventName,tagMetaData,lyl_l_id);
			
			//BOM = Byte-Order-Mark
			//Remove the BOM to make the XML valid
			String xmlWithoutBOM = removeUTF8BOM(customXml);
			out.write(xmlWithoutBOM);
			out.close();

			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			int c;
			while ((c = in.read()) != -1) {
				strBuff.append((char) c); 
			}
			
			//Persist info to Mongo after successfully transmission of message to Oracle.
			LOGGER.info(lyl_l_id+"~~~"+xmlWithoutBOM);
			occasionResponsesDao.addOccasionResponse(l_id, eid, custEventName, tagMetaData.getPurchaseOccasion(), tagMetaData.getBusinessUnit(), tagMetaData.getSubBusinessUnit(), 
					strBuff.toString().contains("<success>true</success>") ? "Y" : "N", tag);
			
			xmlWithoutBOM = null;
			xmlWithoutExpo = null;
			json2XmlString = null;
			tagMetaData = null;
			o = null;
			customXml = null;
			
		} catch (Exception t) {
			t.printStackTrace();
			LOGGER.error("Exception occured in getResponseServiceResult ", t);
		} finally {
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
		System.out.println("Response String ====>" + strBuff.toString());
		LOGGER.info(" exiting the method getResponseServiceResult");
		return strBuff.toString();
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
           System.out.println("Score: " + getCharacterDataFromElement(line));
           
           Node node = name.item(0);
		   // get the score element, and update the value
		   if ("score".equals(node.getNodeName())) {
			   if(node.getTextContent().contains("E")){
				   node.setTextContent( BigDecimal.valueOf(Double.parseDouble(node.getTextContent())).toPlainString());
			   }
		   }
		   
		   //No Longer needed as Total Score has been removed from the API response
		   // get the Totalscore element, and update the value
		   /*name = element.getElementsByTagName("totalScore");
		   node = name.item(0);
		   System.out.println("Total Score: " + getCharacterDataFromElement(line));
		   if ("totalScore".equals(node.getNodeName())) {
			   if(node.getTextContent().contains("E")){
				   node.setTextContent( BigDecimal.valueOf(Double.parseDouble(node.getTextContent())).toPlainString());
			   }
		   }*/
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
		System.out.println("xmlWithoutExpo = " +xmlString);
		
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
	public String createCustomXml(String xml, String emailId, String custEventNm, TagMetadata tagMetaData, String lyl_l_id) 
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
		if(emailId!=null && !emailId.equals(""))
			customerId.appendChild(doc.createTextNode(emailId));
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
		name.appendChild(doc.createTextNode("variable1"));
		optionalData.appendChild(name);
		
		//value element inside of optionalData
		Element value = doc.createElement("value");
		optionalData.appendChild(value);
		value.appendChild(doc.createCDATASection("RTS_DATA"));
		
		//Optional Data for adding the MDTag, BU and SUB_BU
		//optionalData element inside of recipientData
		Element optionalData2 = doc.createElement("optionalData");
		recipientData.appendChild(optionalData2);
		Element name2 = doc.createElement("name");
		name2.appendChild(doc.createTextNode("mdTag"));
		optionalData2.appendChild(name2);
		Element value2 = doc.createElement("value");
		optionalData2.appendChild(value2);
		if(tagMetaData!=null && tagMetaData.getMdTags()!=null && !tagMetaData.getMdTags().equals(""))
			value2.appendChild(doc.createTextNode(tagMetaData.getMdTags()));

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
		String interminStr = xmlString.replace("RTS_DATA", "<RTS> " +xml+ " </RTS>");
		interminStr = interminStr.replace("<start>", "").replace("</start>", "");
		interminStr = interminStr.replace("scoresInfo", "element");
		String finalXmlStr = interminStr.substring(0, interminStr.indexOf("<element>"))+" <scoresInfo> "  
				+ interminStr.substring(interminStr.indexOf("<element>"),interminStr.lastIndexOf("</element>")+10)  
					+ " </scoresInfo> " + interminStr.substring(interminStr.lastIndexOf("</element>")+10,interminStr.length());
		System.out.println("customXml =  "+finalXmlStr);
		
		interminStr = null;
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
			//org.json.JSONObject obj = new org.json.JSONObject(jsonStr);
			tagMetaData = new TagMetadata();
			System.out.println(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("occassion"));
			tagMetaData.setPurchaseOccassion(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("occassion")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("occassion").toString() : null);
			tagMetaData.setBusinessUnit(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("businessUnit")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("businessUnit").toString() : null);
			tagMetaData.setSubBusinessUnit(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("subBusinessUnit")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("subBusinessUnit").toString() : null);
			tagMetaData.setMdTags(((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("mdTag")!= null ? 
					((org.json.JSONObject) obj.getJSONArray("scoresInfo").get(0)).get("mdTag").toString() : null);
			
		} catch (org.json.JSONException e) {
			LOGGER.info(e.getMessage());
		}
		
		return tagMetaData;
	}

	
	private Boolean isMdTagPresentAmongTop5TagsFromAPI(org.json.JSONObject obj, String tag){
		try {
			org.json.JSONArray arr = obj.getJSONArray("scoresInfo");
			
			for(int i=0; (i< arr.length() || i < 5); i++){
				if(((org.json.JSONObject)arr.get(i)).has("mdTag")){
					if(((org.json.JSONObject)arr.get(i)).get("mdTag").toString().equalsIgnoreCase(tag))
						return true;
				}
			}
			
		} catch (org.json.JSONException e) {
			LOGGER.info(e.getMessage());
		}
		return false;
	}
}
