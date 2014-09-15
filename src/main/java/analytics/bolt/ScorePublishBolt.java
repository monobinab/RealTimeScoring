/**
 * 
 */
package analytics.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import redis.clients.jedis.Jedis;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScorePublishBolt extends BaseRichBolt {

	static final Logger logger = LoggerFactory
			.getLogger(ScorePublishBolt.class);
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;
    final String host;
    final int port;
    final String pattern;

    private Jedis jedis;


    public ScorePublishBolt(String host, int port, String pattern) {
        this.host = host;
        this.port = port;
        this.pattern = pattern;
    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
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
		logger.debug("The time it enters inside Score Publish Bolt execute method "+System.currentTimeMillis());
        //System.out.println(" %%% scorepublishbolt :" + input);
        String l_id = input.getStringByField("l_id");
        String modelName = input.getStringByField("model");

        String message = new StringBuffer(l_id).append(",")
            .append(modelName).append(",")
            .append(input.getStringByField("source")).append(",")
            .append(input.getDoubleByField("newScore")).toString();
        //System.out.println(" %%% message : " + message);

        int retryCount=0;
        while(retryCount<5)
        try{
        	jedis.publish(pattern, message);
        	break;
        }
        catch(Exception e){
        	logger.error(e.getMessage(),e);
        	try {
				Thread.sleep(200);
			} catch (InterruptedException e1) {
	        	logger.error(e.getMessage(),e);
	        	outputCollector.fail(input);
			}
        	jedis.publish(pattern, message);
        	retryCount++;
        }
        outputCollector.ack(input);
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
