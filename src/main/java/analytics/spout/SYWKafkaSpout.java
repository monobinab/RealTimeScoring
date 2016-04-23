package analytics.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

public class SYWKafkaSpout extends KafkaSpout{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SYWKafkaSpout.class);

	public SYWKafkaSpout(SpoutConfig spoutConf) {
		super(spoutConf);
	}
	
	 @Override
	    public void fail(Object msgId) {
	 		 LOGGER.error("The message failed messageID:"+msgId.toString());
	 }
		 
	 @Override
		public void nextTuple() {
		 super.nextTuple();
		 LOGGER.info("SYWKafkaSpout Emitting message ");
	 }
}
