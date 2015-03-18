/**
 * 
 */
package analytics.bolt;

import analytics.util.HostPortUtility;
import analytics.util.MongoNameConstants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisCounterBolt extends BaseRichBolt {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
    private Jedis jedis;
    private OutputCollector outputCollector;
    final String host;
    final int port;

    public RedisCounterBolt(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
     //   System.setProperty(MongoNameConstants.IS_PROD, String.valueOf(stormConf.get(MongoNameConstants.IS_PROD)));
		   HostPortUtility.getEnvironment(stormConf.get("nimbus.host").toString());
        jedis = new Jedis(host, port);
        this.outputCollector = collector;
    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {

        String document = (String) input.getValueByField("message");
        jedis.incr(document);


	}

    /*
      * (non-Javadoc)
      *
      * @see
      * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
      * topology.OutputFieldsDeclarer)
      */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
