package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class WebHDFSSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(WebHDFSSpout.class);

	private SpoutOutputCollector collector;
	private JedisPool jedisPool;
	private String host;
	private int port;
	private String hdfsPath;
	private String topologyIdentifier;

	public WebHDFSSpout(String host, int port, String hdfsPath,
			String topologyIdentifier) {
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
		jedisPool = new JedisPool(poolConfig, host, port, 100);

	}

	@Override
	public void nextTuple() {

		Long latestPrefix = 20150305040700L;
		TreeSet<Long> sortedSet = new TreeSet<Long>();
		TreeSet<Long> sortedSubSet = new TreeSet<Long>();
		try {

			Jedis jedis = jedisPool.getResource();
			// jedis.set(topologyIdentifier, latestPrefix.toString());
			latestPrefix = Long.parseLong(jedis.get(topologyIdentifier));
			jedisPool.returnResource(jedis);

			System.setProperty("java.security.krb5.conf", Constants.KERBEROSE_CONF_PATH);
			System.setProperty("keytabPath", Constants.KERBEROSE_KEYTAB_PATH);
		  	//System.setProperty("java.security.krb5.realm", "HADOOP.SEARSHC.COM");
			//System.setProperty("java.security.krb5.kdc", "10.0.29.9");
			Configuration conf = new Configuration();
			conf.set("hadoop.security.authentication", "kerberos");
			conf.set("fs.defaultFS", Constants.WEBHDFS_URL);
			conf.set("fs.webhdfs.impl", org.apache.hadoop.hdfs.web.WebHdfsFileSystem.class.getName());
			conf.set("com.sun.security.auth.module.Krb5LoginModule", "required");
			conf.set("debug", "true");

			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(Constants.WEBHDFS_PRINCIPAL, Constants.KERBEROSE_KEYTAB_PATH);

			String s = null;
			Process p = Runtime
					.getRuntime()
					.exec("kinit -kt "+Constants.KERBEROSE_KEYTAB_PATH+" "+Constants.WEBHDFS_PRINCIPAL);

			BufferedReader stdError = new BufferedReader(new InputStreamReader(
					p.getErrorStream()));

			// read any errors from the attempted command
			LOGGER.error("Here is the standard error of the command (if any):\n");
			while ((s = stdError.readLine()) != null) {
				LOGGER.error(s + " ");
			}

			URI uri = URI.create(Constants.WEBHDFS_URL);
			FileSystem fs = FileSystem.get(uri, conf);
			//New debug logs 
				
            LOGGER.info("The path to be checked is "+hdfsPath );;
			FileStatus fstat[] = fs.listStatus(new Path(hdfsPath));
			if(fstat.length==0)
			{
				LOGGER.info("There are no files in the directory");
			}

			for (FileStatus fst : fstat) {
				LOGGER.info("File name from File status "+fst.getPath().getName());
				sortedSet.add(new Long(fst.getPath().getName()));
			}

			if (sortedSet.contains(latestPrefix)) {
				// System.out.println(sortedSet.tailSet(latestPrefix, false));
				// Get the remaining TO BE PROCESSED prefixes
				LOGGER.info("Adding the remaining TO BE PROCESSED to sortedset");
				sortedSubSet = (TreeSet<Long>) sortedSet.tailSet(latestPrefix,
						false);
			}
			// sortedSubSet.add(20150729L);

			Iterator iter = sortedSubSet.iterator();
			// Process Individual files from the timestamped(prefixed)
			// directory.
			while (iter.hasNext()) {

				String path = (iter.next()).toString();

				FileStatus fstat2[] = fs.listStatus(new Path(hdfsPath + "/"	+ path));

				processRecords(fs, fstat2);
				// Write back to reds that the files in the directory are
				// processed so
				// the next run would not pick it up to process
				jedis = jedisPool.getResource();
				jedis.set(topologyIdentifier, path);
				jedisPool.returnResource(jedis);
			}
			// Sleep for 5 mins before starting the next process
			Thread.sleep(60000);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Error in communication with webhdfs [" + hdfsPath
					+ "]");
		}
	}

	/**
	 * 
	 * @param fs
	 * @param fstat2
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void processRecords(FileSystem fs, FileStatus[] fstat2)
			throws IOException, InterruptedException {
		
		
		
		for (int i = 0; i < fstat2.length; i++) {
			// ignoring files like _SUCCESS
			if (fstat2[i].getPath().getName().startsWith("_")) {
				continue;
			}
			LOGGER.info("In processRecords : about to Process File "+fstat2[i].getPath().getName());
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fs.open(fstat2[i].getPath())));
			String line;
			int count = 0;
			line = br.readLine();
			while (line != null) {
				String str = formatRecordsToFitRespectiveBolts(line);
				System.out.println(str);
				count++;
				// Time to call the BOLT
				collector.emit(tuple(str));
				if (count >= 1000) {
					Thread.sleep(15000);
					count = 0;
				}
				line = br.readLine();
			}
		}
	}

	/**
	 * Call the bolt to process the records
	 * 
	 * @param url
	 * @throws IOException
	 * @throws InterruptedException
	 *//*
	public void readURLAndStoreData(String url) throws IOException,
			InterruptedException {
		URL oracle = new URL(url);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				oracle.openStream()));

		String inputLine;
		int count = 0;
		while ((inputLine = in.readLine()) != null) {
			String str = formatRecordsToFitRespectiveBolts(inputLine);
			count++;
			// Time to call the BOLT
			collector.emit(tuple(str));
			if (count >= 1000) {
				Thread.sleep(15000);
				count = 0;
			}
			str = null;
			inputLine = null;
		}
		in.close();
	}*/

	public String formatRecordsToFitRespectiveBolts(String str) {
		String replacedString = null;
		if (str != null && !str.equals("")) {
			String returnStr = str.replace("\u0001", "', '").replace("\u0002",
					"', '");
			// returnStr.replace(",", "',");
			if (topologyIdentifier.equalsIgnoreCase("aamTraits")) {
				returnStr = returnStr.substring(0, returnStr.indexOf(",") + 1)
						+ " \"["
						+ returnStr.substring(returnStr.indexOf(",") + 1,
								returnStr.length());
				returnStr = returnStr.substring(0, returnStr.length()) + "']\"";
				replacedString = "['" + returnStr + "]";
			} else if (topologyIdentifier.equalsIgnoreCase("aamInternalSearch")) {
				replacedString = str;
			} else {
				replacedString = returnStr;
			}
			LOGGER.info("Formatted String = " + replacedString);
			returnStr = null;
		}

		return replacedString;
	}

}