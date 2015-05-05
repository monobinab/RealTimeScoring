package analytics.spout;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import analytics.util.MongoNameConstants;
import analytics.util.objects.Vibes;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class VibesSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(VibesSpout.class);
	//private VibesDao vibesDao;
	private String host;
	private int port;

    private SpoutOutputCollector collector;
	
	public VibesSpout(String systemProperty, String redisServer, Integer redisPort) {
			System.setProperty(MongoNameConstants.IS_PROD, systemProperty);
			this.host = redisServer;
			this.port = redisPort;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("vibesDBObject"));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		//vibesDao = new VibesDao();
	}

	@Override
	public void nextTuple() {
		Jedis jedis = null;
		try{
			
			Date date = new Date();
			
			String startTime = (new SimpleDateFormat("yyyy-MM-dd").format(date))+" 10:00:00";
			Date startTimeToday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime);
			
			String endTime = (new SimpleDateFormat("yyyy-MM-dd").format(date))+" 17:00:00";
			Date endTimeToday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endTime);

			//Perform Vibes Text Processing between 10:00AM and 4:00PM CST
			if(date.after(startTimeToday) && date.before(endTimeToday)){
			
				//List<DBObject> vibesLst = vibesDao.getVibes(Constants.NO);
				//Iterator<DBObject> iter = vibesLst.iterator();
				
				jedis = new Jedis(host,port,900);
				Set<String> names=jedis.keys("Vibes:*");
				Vibes vibesObj = null;
				
				Iterator<String> it = names.iterator();
			    while (it.hasNext()) {
			        
			    	String s = it.next();
			        String subStr = s.substring(s.indexOf("Vibes:")+6, s.length());
			        
			        vibesObj = new Vibes();
			        vibesObj.setLyl_id_no(subStr);
			        vibesObj.setEvent_type(jedis.get(s));
			        ArrayList<Object> listToEmit = new ArrayList<Object>();
			        listToEmit.add(vibesObj);
			        
					this.collector.emit(listToEmit);
					
					//jedis.del(s);
					
					s = null;
					listToEmit = null;
					vibesObj = null;
					subStr = null;
					
			    }
				
			}
			//Sleep for 3 mins before starting the next batch
			Thread.sleep(180000);
		}
		catch(Exception e){
			LOGGER.error("Error in Vibes Spout ");
		}finally{
			if(jedis !=null)
				jedis.disconnect();
		}
	}
	
	
}
