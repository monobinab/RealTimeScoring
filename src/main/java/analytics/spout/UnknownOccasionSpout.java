package analytics.spout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import analytics.util.MongoNameConstants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class UnknownOccasionSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(UnknownOccasionSpout.class);
	private String host;
	private int port;

    private SpoutOutputCollector collector;

	public UnknownOccasionSpout(String systemProperty, String redisServer, Integer redisPort) {
			System.setProperty(MongoNameConstants.IS_PROD, systemProperty);
			this.host = redisServer;
			this.port = redisPort;
	}


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Jedis jedis = null;
		try{
				jedis = new Jedis(host,port,900);
				Set<String> names=jedis.keys("Unknown:*");

				Iterator<String> it = names.iterator();
			    while (it.hasNext()) {

			    	String s = it.next();
			    	String loyaltyId = s.substring(s.indexOf("Unknown:")+8, s.length());

			        ArrayList<Object> listToEmit = new ArrayList<Object>();
			        listToEmit.add(loyaltyId);
			        this.collector.emit(listToEmit);
					jedis.del(s);
					loyaltyId = null;
					s = null;
					listToEmit = null;

			    }
			    jedis.disconnect();

			//Sleep for 3 mins before starting the next batch
			Thread.sleep(180000);
		}
		catch(Exception e){
			LOGGER.error("Error in UnknownOccasion Spout ");
		}finally{
			if(jedis !=null)
				jedis.disconnect();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lyl_id_no"));
	}

}