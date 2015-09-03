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
import backtype.storm.command.list;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class ResponsysSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ResponsysSpout.class);
	private String host;
	private int port;
	private String topologyName;
	private String searchKey;

    private SpoutOutputCollector collector;
    protected MultiCountMetric countMetric;

	public ResponsysSpout(String systemProperty, String redisServer, Integer redisPort) {
			System.setProperty(MongoNameConstants.IS_PROD, systemProperty);
			this.host = redisServer;
			this.port = redisPort;
	}


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		topologyName = (String) conf.get("metrics_topology");
		
		countMetric = new MultiCountMetric();
		context.registerMetric("custome_metrics", countMetric, 60);
		
		//Determine the Search Key to find on Redis
		if(topologyName.equalsIgnoreCase(Constants.UNKNOWN_OCCASION))
			this.searchKey = "Unknown";
		else if (topologyName.equalsIgnoreCase(Constants.POS_PURCHASE))
			this.searchKey = "Pos";
		
	}

	@Override
	public void nextTuple() {
		Jedis jedis = null;
		try{
				jedis = new Jedis(host,port,900);

				Set<String> names=jedis.keys(searchKey+":*");

				Iterator<String> it = names.iterator();
			    while (it.hasNext()) {

			    	countMetric.scope("incoming_tuples").incr();
			    	String s = it.next();
			    	String loyaltyId = s.substring(s.indexOf(searchKey)+searchKey.length()+1, s.length());
			    	
			    	String value = jedis.get(s);

			        ArrayList<Object> listToEmit = new ArrayList<Object>();
			        listToEmit.add(loyaltyId);
			        listToEmit.add(value);
			        
			        this.collector.emit(listToEmit);
			        jedis.del(s);
					loyaltyId = null;
					value = null;
					s = null;
					listToEmit = null;
					
			    }
			    jedis.disconnect();

			//Sleep for 3 mins before starting the next batch
			Thread.sleep(180000);
		}
		catch(Exception e){
			LOGGER.error("Error in ResponsysSpout Spout from the Topology : " + topologyName);
		}finally{
			if(jedis !=null)
				jedis.disconnect();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lyl_id_no","value"));
	}

}