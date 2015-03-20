package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class OccassionRedisSpout extends RedisPubSubSpout{
	public OccassionRedisSpout(int number, String pattern, String systemProperty) {
	super(number, pattern, systemProperty)	;
	}
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(OccassionRedisSpout.class);
	
	@Override
	protected void emit(String ret) {
		// TODO: Find why there was a sleep?? Copied form AAMTopology
		try {
			TimeUnit.MILLISECONDS.sleep(2);
		} catch (InterruptedException e) {
			LOGGER.error("Thread can not be interrupted",e);
		}
		if (ret != null) {
		//	System.out.println(ret);
			_collector.emit(tuple(ret));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}


}
