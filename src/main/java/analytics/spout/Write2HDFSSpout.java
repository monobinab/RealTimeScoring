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
import org.json.JSONException;
import org.json.JSONObject;
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

public class Write2HDFSSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(Write2HDFSSpout.class);
	static String LISTSTATUS_WEBHDFS_URL="http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>?user.name=spannal&op=LISTSTATUS";
	static String CONTENT_SUMMARY_URL = "http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>/<PATH>?user.name=spannal&op=GETCONTENTSUMMARY";
	static String FILE_READ_URL = "http://151.149.131.21:14000/webhdfs/v1<HDFS_LOCATION>/<PATH>?user.name=spannal&op=OPEN";
	static String FILE_WRITE_URL = "http://151.149.131.21:14000/webhdfs/v1<PATH>?user.name=spannal&op=CREATE";
	static String FILE_APPEND_URL = "http://151.149.131.21:14000/webhdfs/v1<PATH>?user.name=spannal&op=APPEND";
	static String FILE_STATUS_URL = "http://151.149.131.21:14000/webhdfs/v1<PATH>?user.name=spannal&op=GETFILESTATUS";
	static String WEBHDFS_URL = "http://151.149.131.21:14000";
	static String UNIVERSAL_USER = "rtsadmin";

    private SpoutOutputCollector collector;
	private JedisPool jedisPool;
	private String host;
	private int port;
	private String hdfsPath;
	private String topologyIdentifier;
	private String write2HdfsPath;
	private String write2HdfsFile;
	
	public Write2HDFSSpout(String host, int port, String hdfsPath, String topologyIdentifier) {
		this.host = host;
		this.port = port;
		this.hdfsPath = hdfsPath;
		this.topologyIdentifier = topologyIdentifier;
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
			latestPrefix =  Long.parseLong( jedis.get(topologyIdentifier))  ;
			jedisPool.returnResource(jedis);
			
			String hdfsUrl = LISTSTATUS_WEBHDFS_URL.replace("<HDFS_LOCATION>", hdfsPath);
			
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
				//String currentURL = CONTENT_SUMMARY_URL.replace("<PATH>", path).replace("<HDFS_LOCATION>", hdfsPath);
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
					readURLAndWriteToHDFS(currentURL);
				}
				
				//Write back to reds that the files in the directory are processed so 
				//the next run would not pick it up to process
				jedis = jedisPool.getResource();
				jedis.set(topologyIdentifier, latestPrefix.toString());
				jedisPool.returnResource(jedis);
				
				files = null;
				filesArray = null;
				currentURL = null;
				path = null;
			}
			iter = null;
			//Sleep for 5 mins before starting the next process
			//Thread.sleep(300000);
		}
		catch(Exception e){
			LOGGER.error("Error in communication with webhdfs ["+hdfsPath+"]");
		}
	}
	
	
	/**
	 *  Call the bolt to process the records
	 * @param url
	 * @throws IOException
	 * @throws JSONException 
	 */
	public void readURLAndWriteToHDFS(String url) throws IOException, JSONException{
		URL oracle = new URL(url);
        BufferedReader in = new BufferedReader(
        new InputStreamReader(oracle.openStream()));

        String inputLine;
        while ((inputLine = in.readLine()) != null){
        	String str = formatRecordsToFitRespectiveBolts(inputLine);
        	
        	 collector.emit(tuple(str));
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
			LOGGER.info("Formatted String = " +replacedString);
			returnStr = null;
		}
		
		return replacedString;
	}
	
	public void write2HDFS(String inputString) throws JSONException{
		//Check If the file already exists
		String fileStatusURL = FILE_STATUS_URL.replace("<PATH>", write2HdfsFile);
		
		try{
			JSONObject obj= HttpClientUtils.httpGetCall(fileStatusURL);
			if(!obj.has("FileStatus")){
				String fileCreate = FILE_WRITE_URL.replace("<PATH>", write2HdfsFile);
			
				ProcessBuilder pb = new ProcessBuilder(
		            "curl",
		            "-i",
		            "-X POST "+fileCreate);
			
				

			}
			
		}catch (Exception e){
			System.out.println("Exception Occurred. File does not exist" );
			e.printStackTrace();
		}
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("logger_message"));
	}

}
