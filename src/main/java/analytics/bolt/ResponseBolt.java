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
import analytics.util.ResponsysUtil;
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
	private String host;
	private int port;
	private JedisPool jedisPool;
	private ResponsysUtil responsysUtil;
	
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
		this.outputCollector = collector;
		responsysUtil = new ResponsysUtil();
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
				String scoreInfoJsonString = responsysUtil.callRtsAPI(lyl_id_no);
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
						responsysUtil.getResponseServiceResult(scoreInfoJsonString,lyl_id_no,tag);
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
	
}
