package analytics.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

public class RTSKafkaSpout extends KafkaSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(RTSKafkaSpout.class);

	public RTSKafkaSpout(SpoutConfig spoutConf) {
		super(spoutConf);

	}
	/* This method is overridden empty to disable spout from reading 
	the kafka message again in case of failure */
	 @Override
	    public void fail(Object msgId) {
		 
	 }
	

}
