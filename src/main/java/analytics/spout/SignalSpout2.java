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
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.objects.RtsCommonObj;
import analytics.util.objects.Vibes;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class SignalSpout2 extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SignalSpout2.class);
	private String host;
	private int port;

    private SpoutOutputCollector collector;
    private String topologyName;
    private String searchKey;
	
	public SignalSpout2(String systemProperty, String redisServer, Integer redisPort) {
			System.setProperty(MongoNameConstants.IS_PROD, systemProperty);
			this.host = redisServer;
			this.port = redisPort;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rtsCommonObj"));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		topologyName = (String) conf.get("metrics_topology");
		if(topologyName.equalsIgnoreCase(Constants.SIGNAL_TOPOLOGY2))
			this.searchKey = "signal";
	}

	@Override
	public void nextTuple() {
		Jedis jedis = null;
		try{
			
			/*Date date = new Date();
			
			String startTime = (new SimpleDateFormat("yyyy-MM-dd").format(date))+" 10:00:00";
			Date startTimeToday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime);
			
			String endTime = (new SimpleDateFormat("yyyy-MM-dd").format(date))+" 17:00:00";
			Date endTimeToday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endTime);

			//Perform Vibes Text Processing between 10:00AM and 4:00PM CST
			if(date.after(startTimeToday) && date.before(endTimeToday)){*/
			
				//List<DBObject> vibesLst = vibesDao.getVibes(Constants.NO);
				//Iterator<DBObject> iter = vibesLst.iterator();
				
				jedis = new Jedis(host,port,900);
				Set<String> names=jedis.keys(searchKey+":*");
				RtsCommonObj commonObj = null;
				
				Iterator<String> it = names.iterator();
			    while (it.hasNext()) {
			        
			    	String s = it.next();
			        //String subStr = s.substring(s.indexOf("signal:")+6, s.length());
			        String loyaltyId = s.substring(s.indexOf(searchKey)+searchKey.length()+1, s.length());
			        
			        commonObj = new RtsCommonObj();	
			        commonObj.setLyl_id_no(loyaltyId);
			        commonObj.setPidList(new ArrayList<String> (jedis.lrange(s, 0,-1)));

			        if(shouldProceedWithProcessing(commonObj)){
				        ArrayList<Object> listToEmit = new ArrayList<Object>();
				        listToEmit.add(commonObj);
						this.collector.emit(listToEmit);
						listToEmit = null;
						jedis.del(s);
			        }

					s = null;
					commonObj = null;
			    }
			    jedis.disconnect();
				
			//}
			//Sleep for 3 mins before starting the next batch
			Thread.sleep(180000);
		}
		catch(Exception e){
			LOGGER.error("Error in SignalSpout2 ");
		}finally{
			if(jedis !=null)
				jedis.disconnect();
		}
	}
	
	private boolean shouldProceedWithProcessing(RtsCommonObj commonObj){
		
		ArrayList<String> lst = commonObj.getPidList();
		Long timeInsertedIntoRedis = new Long (lst.get(0));
		if(System.currentTimeMillis()-timeInsertedIntoRedis >= 5000)
			return true;
	
		return false;
	}
	
	
}
