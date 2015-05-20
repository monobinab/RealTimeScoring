package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.util.Constants;
import analytics.util.HttpClientUtils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class WebHDFSSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(WebHDFSSpout.class);

    private SpoutOutputCollector collector;
	private JedisPool jedisPool;
	private String host;
	private int port;
	private String hdfsPath;
	private String topologyIdentifier;
	
	public WebHDFSSpout(String host, int port, String hdfsPath, String topologyIdentifier) {
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
			
			Jedis jedis = jedisPool.getResource();
			//jedis.set(topologyIdentifier, latestPrefix.toString());
			latestPrefix =  Long.parseLong( jedis.get(topologyIdentifier))  ;
			jedisPool.returnResource(jedis);
			
			String hdfsUrl = Constants.LIST_STATUS_WEBHDFS_URL.replace("<HDFS_LOCATION>", hdfsPath);
			
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
				String currentURL = Constants.LIST_STATUS_WEBHDFS_URL.replace("<HDFS_LOCATION>", hdfsPath+"/"+path);
				JSONArray filesArray = (HttpClientUtils.httpGetCall(currentURL)
						.getJSONObject("FileStatuses").getJSONArray("FileStatus"));
				
				//Only for simplicity the iteration has been performed and stored in a string array.
				//This could also have been done in single iteration 
				ArrayList<String> files = new ArrayList<String>();
				for (int i=0; i< filesArray.length(); i++){
					files.add(filesArray.getJSONObject(i).getString("pathSuffix"));
				}
				System.out.println("# of File to process = "+files.size());
				
				for(int i=0;i<files.size();i++){
					currentURL = Constants.FILE_READ_WEBHDFS_URL.replace("<PATH>", files.get(i)).replace("<HDFS_LOCATION>", hdfsPath+"/"+path);
					System.out.println("File being Processed = " + currentURL);
					readURLAndStoreData(currentURL);
				}
				
				
				/*String currentURL = Constants.CONTENT_SUMMARY_URL.replace("<PATH>", path).replace("<HDFS_LOCATION>", hdfsPath);
				Integer fileCount = HttpClientUtils.httpGetCall(currentURL).getJSONObject("ContentSummary").getInt("fileCount");
				for(int i=0;i<fileCount;i++){
					if(topologyIdentifier.equalsIgnoreCase("aamTraits") || topologyIdentifier.equalsIgnoreCase("aamBrowser"))
						currentURL = Constants.FILE_READ_URL.replace("<PATH>", path+"/00000"+i+"_0").replace("<HDFS_LOCATION>", hdfsPath);
					else
						currentURL = Constants.FILE_READ_URL.replace("<PATH>", path+"/part-m-0000"+i+"_0").replace("<HDFS_LOCATION>", hdfsPath);
					readURLAndStoreData(currentURL);
				}*/
				
				//Write back to reds that the files in the directory are processed so 
				//the next run would not pick it up to process
				jedis = jedisPool.getResource();
				jedis.set(topologyIdentifier, path);
				jedisPool.returnResource(jedis);
			}
			//Sleep for 5 mins before starting the next process
			Thread.sleep(60000);
		}
		catch(Exception e){
			LOGGER.error("Error in communication with webhdfs ["+hdfsPath+"]");
		}
	}
	
	
	/**
	 *  Call the bolt to process the records
	 * @param url
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void readURLAndStoreData(String url) throws IOException, InterruptedException{
		URL oracle = new URL(url);
        BufferedReader in = new BufferedReader(
        new InputStreamReader(oracle.openStream()));

        String inputLine;
        int count =0;
        while ((inputLine = in.readLine()) != null){
        	String str = formatRecordsToFitRespectiveBolts(inputLine);
        	count++;
        	//Time to call the BOLT
        	 collector.emit(tuple(str));
        	 if(count >= 1000){
        		 Thread.sleep(15000);
        		 count =0;
        	 }
        	 str = null;
        	 inputLine = null;
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
		   else if(topologyIdentifier.equalsIgnoreCase("aamInternalSearch")){
		    returnStr = returnStr.replace("null", "");
		    String[] splitStr = returnStr.split(",");
		    returnStr = splitStr[2]+"', '"+splitStr[0]+"', '"+splitStr[1]+"', '"+splitStr[5];
		    replacedString = "['"+returnStr+"']";
		    splitStr = null;
		   }
		   else
		    replacedString = "['"+returnStr+"']";
		   //LOGGER.info("Formatted String = " +replacedString);
		   System.out.println("Formatted String = " +replacedString);
		   returnStr = null;
		  }
		  
		  return replacedString;
		 }

}
