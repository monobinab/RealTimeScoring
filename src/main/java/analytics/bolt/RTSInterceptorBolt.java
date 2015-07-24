/**
 * 
 */
package analytics.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @author pnair0
 *
 */
public class RTSInterceptorBolt extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(RTSInterceptorBolt.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector outputCollector;

	public RTSInterceptorBolt(String environment)
			throws ConfigurationException {
		super(environment);
		
	}

	@Override
	public void execute(Tuple input) {
		List<Object> listToEmit = new ArrayList<Object>();	
		//Kafka message is extracted from position 0
		listToEmit.add(input.getValue(0));
		//A message ID is added as in REDIS Spout
		listToEmit.add(new Double(Math.random()).toString());

		//redisCountIncr("RTSInterceptorBolt_count");	
		LOGGER.info("Emitting Record in RTSInterceptorBolt "+input.getValue(0) );

		this.outputCollector.emit(listToEmit);
		outputCollector.ack(input);
	}


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		LOGGER.info("RTSInterceptorBolt Preparing to Launch");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("message", "messageID"));
	}

}
