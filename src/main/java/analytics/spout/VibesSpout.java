package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.derby.iapi.services.classfile.CONSTANT_Index_info;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import analytics.util.Constants;
import analytics.util.HttpClientUtils;
import analytics.util.MongoNameConstants;
import analytics.util.dao.MemberMDTagsDao;
import analytics.util.dao.VibesDao;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class VibesSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(VibesSpout.class);
	private VibesDao vibesDao;

    private SpoutOutputCollector collector;
	
	public VibesSpout(String systemProperty) {
			LOGGER.info("~~~~~~~~~~~~~~~ENVIRONMENT BOLT~~~~~~~: " + System.getProperty(MongoNameConstants.IS_PROD));
			System.setProperty(MongoNameConstants.IS_PROD, systemProperty);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("vibesDBObject"));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		vibesDao = new VibesDao();
	}

	@Override
	public void nextTuple() {

		try{
			
			Date date = new Date();
			
			String startTime = (new SimpleDateFormat("yyyy-MM-dd").format(date))+" 10:00:00";
			Date startTimeToday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime);
			
			String endTime = (new SimpleDateFormat("yyyy-MM-dd").format(date))+" 16:00:00";
			Date endTimeToday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endTime);

			//Perform Vibes Text Processing between 10:00AM and 4:00PM CST
			if(date.after(startTimeToday) && date.before(endTimeToday)){
			
				List<DBObject> vibesLst = vibesDao.getVibes(Constants.NO);
				Iterator<DBObject> iter = vibesLst.iterator();
				
				while(iter.hasNext()){
					collector.emit(tuple(iter.next()));
				}
			}
			//Sleep for 3 mins before starting the next batch
			Thread.sleep(180000);
		}
		catch(Exception e){
			LOGGER.error("Error in Vibes Spout ");
		}
	}
	
}
