package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.util.HttpClientUtils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TraitsSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TraitsSpout.class);
	static String HADOOP_WEBHDFS_URL="http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>?user.name=spannal&op=LISTSTATUS";
	static String CONTENT_SUMMARY_URL = "http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>/<PATH>?user.name=spannal&op=GETCONTENTSUMMARY";
	static String FILE_READ_URL = "http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>/<PATH>?user.name=spannal&op=OPEN";

    private SpoutOutputCollector collector;
	private JedisPool jedisPool;
	private String host;
	private int port;
	private String hdfsPath;
	private String topologyIdentifier;
	
	public TraitsSpout(String host, int port, String hdfsPath, String topologyIdentifier) {
		this.host = host;
		this.port = port;
		this.hdfsPath = hdfsPath;
		this.topologyIdentifier = topologyIdentifier;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.collector = collector;
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(100);
        jedisPool = new JedisPool(poolConfig,host, port, 100);
 
	}

	@Override
	public void nextTuple() {

		Long latestPrefix = 20150305040700L;
		TreeSet<Long> sortedSet = new TreeSet<Long>();
		TreeSet<Long> sortedSubSet = new TreeSet<Long>();
		try{
			//Sleep for 5 mins before starting the next process
			Thread.sleep(300000);
			
			Jedis jedis = jedisPool.getResource();
			//jedis.set(topologyIdentifier, latestPrefix.toString());
			latestPrefix =  Long.parseLong( jedis.get(topologyIdentifier))  ;
			jedisPool.returnResource(jedis);
			
			String hdfsUrl = HADOOP_WEBHDFS_URL.replace("<HDFS_LOCATION>", hdfsPath);
			
			JSONArray arr = (HttpClientUtils.httpGetCall(hdfsUrl)
					.getJSONObject("FileStatuses").getJSONArray("FileStatus"));
			
			for (int i=0; i< arr.length(); i++){
				sortedSet.add( arr.getJSONObject(i).getLong("pathSuffix"));
			}
			
			if(sortedSet.contains(latestPrefix)){
				//System.out.println(sortedSet.tailSet(latestPrefix, false));
				//Get the remaining TO BE PROCESSED prefixes
				sortedSubSet = (TreeSet<Long>) sortedSet.tailSet(latestPrefix, false);
			}
			
			Iterator iter = sortedSubSet.iterator();
			//Process Individual files from the timestamped(prefixed) directory.
			while(iter.hasNext()){
				String path = (iter.next()).toString();
				String currentURL = CONTENT_SUMMARY_URL.replace("<PATH>", path).replace("<HDFS_LOCATION>", hdfsPath);
				/*System.out.println(currentURL);*/
				Integer fileCount = HttpClientUtils.httpGetCall(currentURL).getJSONObject("ContentSummary").getInt("fileCount");
				System.out.println(fileCount);
				
				for(int i=0;i<fileCount;i++){
					currentURL = FILE_READ_URL.replace("<PATH>", path+"/00000"+i+"_0").replace("<HDFS_LOCATION>", hdfsPath);
					readURLAndStoreData(currentURL);
				}
				
				//Write back to reds that the files in the directory are processed so 
				//the next run would not pick it up to process
				jedis = jedisPool.getResource();
				jedis.set(topologyIdentifier, latestPrefix.toString());
				jedisPool.returnResource(jedis);
			}
		}
		catch(Exception e){
			LOGGER.error("Error in communication with webhdfs ["+hdfsPath+"]");
		}
	}
	
	
	/**
	 *  Call the bolt to process the records
	 * @param url
	 * @throws IOException
	 */
	public void readURLAndStoreData(String url) throws IOException{
		URL oracle = new URL(url);
        BufferedReader in = new BufferedReader(
        new InputStreamReader(oracle.openStream()));

        String inputLine;
        while ((inputLine = in.readLine()) != null){
        	String str = formatRecordsToFitRespectiveBolts(inputLine);
        	
        	//Time to call the BOLT
        	 collector.emit(tuple(str));
        }
        in.close();
	}
	
	public String formatRecordsToFitRespectiveBolts(String str){
		String replacedString = null;
		if(str!=null && !str.equals("")){
			String returnStr = str.replace("\u0001","', '").replace("\u0002","', '");
			//returnStr.replace(",", "',");
			if(topologyIdentifier.equalsIgnoreCase("aamTraits")){
				returnStr = returnStr.substring(0, returnStr.indexOf(",")+1)+" \"[" +returnStr.substring(returnStr.indexOf(",")+1, returnStr.length());
				returnStr = returnStr.substring(0, returnStr.length())+"']\"";
				replacedString = "['"+returnStr+"]";
			}
			else
				replacedString = "['"+returnStr+"']";
			System.out.println(returnStr);
		}
		
		return replacedString;
	}

}
