package analytics.spout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.objects.RtsCommonObj;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class SignalBrowseSpout extends BaseRichSpout{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SignalBrowseSpout.class);
	private String host;
	private int port;

    private SpoutOutputCollector collector;
    private String topologyName;
    private String searchKey;
	
	public SignalBrowseSpout(String systemProperty, String redisServer, Integer redisPort) {
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
		if(topologyName.equalsIgnoreCase(Constants.SIGNAL_BROWSE_TOPOLOGY))
			this.searchKey = "signalBrowseFeed";
	}

	@Override
	public void nextTuple() {
		Jedis jedis = null;
		try{
			
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
				//		jedis.del(s);
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
			LOGGER.error("Error in SignalBrowseSpout ", e);
		}finally{
			if(jedis !=null)
				jedis.disconnect();
		}
	}
	
	private boolean shouldProceedWithProcessing(RtsCommonObj commonObj){
		
		ArrayList<String> lst = commonObj.getPidList();
		try{
			Long timeInsertedIntoRedis = new Long (lst.get(0));
			if(System.currentTimeMillis()-timeInsertedIntoRedis >= 5000)
				return true;
		}
		catch(Exception e){
			LOGGER.error("Exception occured in shouldProceedWithProcessing SignalBrowseSpout for lid " + commonObj.getLyl_id_no(), e);
			LOGGER.info("Processing lid " + commonObj.getLyl_id_no() + " after Exception caught");
			ArrayList<String> modLists = commonObj.getPidList();
			modLists.add(0, "");
			commonObj.setPidList(modLists);
			return true;
		}
		return false;
	}
	
	
}
