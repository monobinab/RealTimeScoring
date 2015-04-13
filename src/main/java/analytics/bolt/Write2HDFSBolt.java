package analytics.bolt;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import analytics.util.AlphanumComparator;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.HttpClientUtils;
import analytics.util.PseudoWebHDFSConnection;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Write2HDFSBolt extends BaseRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(Write2HDFSBolt.class);
	private MultiCountMetric countMetric;
	private OutputCollector outputCollector;
	private static final String UTF8_BOM = "\uFEFF";
	private String outputDirectory;
	private String outputFileName;
	private Long fileSize;
	private StringBuilder sb = new StringBuilder();
	private int count = 0;

	
	public Write2HDFSBolt(String outputDirectory, String outputFileName, Long sizeInBytes) {
		this.outputDirectory = outputDirectory;
		this.outputFileName = outputFileName;
		this.fileSize = sizeInBytes;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		initMetrics(context);
		this.outputCollector = collector;

	}
	
	void initMetrics(TopologyContext context){
	     countMetric = new MultiCountMetric();
	     context.registerMetric("custom_metrics", countMetric, 60);
	    }


	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		
		PseudoWebHDFSConnection pConn = null;
		
		//if(input != null && input.contains("logger_message")){
			sb.append(input.getString(0)+"\n");
			count ++;
		//}
		if(count == 100){
			
			pConn = new PseudoWebHDFSConnection(Constants.WEBHDFS_URL, AuthPropertiesReader
					.getProperty(Constants.WEBHDFS_USERNAME), AuthPropertiesReader
					.getProperty(Constants.WEBHDFS_PASSWORD));
			
			System.out.println("Writing to logs");
			writeLogs(sb.toString(), pConn);
			sb = null;
			sb = new StringBuilder();
			count =0;
			pConn = null;
		}
		
		
		//write2File(input.getString(0), "log.txt");
		
		outputCollector.ack(input);
	}
	
	public void writeLogs(String logMessage, PseudoWebHDFSConnection pConn){
		try {
			String tempFileName = outputFileName+".0";
			
			InputStream stream = new ByteArrayInputStream(logMessage.getBytes());
			pConn.append(outputDirectory+"/"+tempFileName, stream);
			
			/*if(logMessage != null && !"".equals(logMessage)){
				InputStream stream = new ByteArrayInputStream(logMessage.getBytes());
				Boolean file_directory_Status = pConn.isExists(outputDirectory);
				
				try{
					if(file_directory_Status == null || !file_directory_Status){
						pConn.mkdirs(outputDirectory);
						pConn.create(outputDirectory+"/"+tempFileName, stream);
					}
					else{
						//check for file exists,
						file_directory_Status = pConn.isExists(outputDirectory+"/"+tempFileName);
						
						if(file_directory_Status == null || !file_directory_Status)
							pConn.create(outputDirectory+"/"+tempFileName, stream);

						else{
							//Coming here means the the directory exists and file exists as well...
							String file_directory_List_Status = pConn.listStatus(outputDirectory);
							JSONObject obj = new JSONObject(file_directory_List_Status);
							HashMap<String,Long> hm = new HashMap<String,Long>();
							
							JSONArray arr = (obj.getJSONObject("FileStatuses").getJSONArray("FileStatus"));
							for (int i=0; i< arr.length(); i++){
								hm.put(arr.getJSONObject(i).getString("pathSuffix"), new Long(arr.getJSONObject(i).getLong("length")));
							}
							
							List<String> list = new ArrayList<String>(hm.keySet());
							Collections.sort(list,new AlphanumComparator());
							
							Long latestFileSize = hm.get(list.get(list.size()-1)); 
							String latestFileName = list.get(list.size()-1);

							//if files size >= input Filesize then rename by incrementing 
							//the numeric digit at the end of the files respectively
							if(latestFileSize >=fileSize){
								
								Long fileAppeneder = new Long(latestFileName.substring(latestFileName.lastIndexOf(".")+1, latestFileName.length())) + 1;
								pConn.create(outputDirectory+"/"+outputFileName+"."+(fileAppeneder), stream);
								
							}
							//The file exists but it is not over the filesize limit
							else
								pConn.append(outputDirectory+"/"+latestFileName, stream);
							
							list = null;
							hm = null;
							obj = null;
							arr = null;
							file_directory_List_Status = null;
						}
					}
					
				}catch (Exception e){
					System.out.println("Exception Occurred. execute method of Write2HDFS" );
					e.printStackTrace();
				}
			}*/
		} catch (Exception e) {
			LOGGER.error("Json Exception ", e);
			countMetric.scope("responses_failed").incr();
		}
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	private void write2File(String logMessage, String fileName){
		PrintWriter out = null;
		try {
		    String tmpFileName = fileName+".0";
		    String logPath = "logs\\";
			
		    List<String> fileNames = new ArrayList<String>();
		    //If this pathname does not denote a directory, then listFiles() returns null. 
		    File[] files = new File(logPath).listFiles();
		    if(files!=null){
			    for (File file : files) {
			        if (file.isFile()) {
			        	fileNames.add(file.getName());
			        	file = null;
			        }
			    }
		    }
		    
		    if(fileNames != null && fileNames.size()>0){
			    Collections.sort(fileNames,new AlphanumComparator());
		    }
		    else{
		    	fileNames.add(tmpFileName);
		    }
		    
		    String fileNm = logPath+fileNames.get(fileNames.size()-1);
		    File file =new File(fileNm);
		    
		    if(file.exists()){
		    	double bytes = file.length();
		    	double kilobytes = (bytes / 1024);
				double megabytes = (kilobytes / 1024);
				//double size = 1000;
				
				if(kilobytes > 1000){
					Long fileAppeneder = new Long(fileNm.substring(fileNm.lastIndexOf(".")+1, fileNm.length())) + 1;
					InputStream stream = (new FileInputStream(file));
					//Create New file
					file =new File(logPath+fileName+"."+(fileAppeneder));
					file.getParentFile().mkdir();
			    	file.createNewFile();
					out = new PrintWriter(new BufferedWriter(new FileWriter(file.getName(), true)));
					out.println(logMessage);
					
					PseudoWebHDFSConnection pConn = new PseudoWebHDFSConnection(Constants.WEBHDFS_URL, AuthPropertiesReader
							.getProperty(Constants.WEBHDFS_USERNAME), AuthPropertiesReader
							.getProperty(Constants.WEBHDFS_PASSWORD));

					pConn.create(outputDirectory+"/"+fileNames.get(fileNames.size()-1), stream);
					
					pConn = null;
					
					//TODO ..delete the file after writing to hdfs
					
				}
				else
					out = new PrintWriter(new BufferedWriter(new FileWriter(fileNm, true)));
		    }
		    else{
		    	file.getParentFile().mkdir();
		    	file.createNewFile();
				out = new PrintWriter(new BufferedWriter(new FileWriter(fileNm, true)));
		    }
		    
		    file = null;
		    fileNames.clear();
		    out.println(logMessage);
		    
		} catch (IOException e) {
		    e.printStackTrace();
		} catch (AuthenticationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(out != null){
				out.close();
				out = null;
			}
		}
		
	}
}
