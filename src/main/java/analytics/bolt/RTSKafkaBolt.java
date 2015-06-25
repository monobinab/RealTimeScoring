/**
 * 
 */
package analytics.bolt;

import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.KafkaUtil;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * @author pnair0
 *
 */
public class RTSKafkaBolt extends EnvironmentBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(RTSKafkaBolt.class);
	private static final long serialVersionUID = 1L;
	private static final String KAFKA_MSG="message";
	private OutputCollector outputCollector;
	private String currentTopic;
	private String env;

	public RTSKafkaBolt(String environment, String topic){
		super(environment);
		this.currentTopic = topic;
		env = environment;		
	}

	/*
	 * -- Pass the message to be sent over to Kafka as the value of "message" field
	 */
	@Override
	public void execute(Tuple input) {
		if (input.contains(KAFKA_MSG)) {
			String message = input.getStringByField(KAFKA_MSG);

			if (message != null && !"".equals(message)) {
				try {
					
					KafkaUtil.sendKafkaMSGs(message, currentTopic);
					
				} catch (ConfigurationException e) {
					LOGGER.error(e.getMessage(), e);
					outputCollector.fail(input);
				}

			}
		}
		else
		{
			LOGGER.error("Kafka message is missing in the input Tuple");
			outputCollector.fail(input);
		}


		outputCollector.ack(input);
	}



	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.outputCollector = collector;
		KafkaUtil.initiateKafkaProperties(env);
		LOGGER.info("RTSKafkaBolt Preparing to Launch");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
