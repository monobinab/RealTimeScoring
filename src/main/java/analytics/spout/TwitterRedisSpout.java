package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class TwitterRedisSpout extends RedisPubSubSpout {
	public TwitterRedisSpout(int number, String pattern) {
		super(number, pattern);
	}
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TwitterRedisSpout.class);
	private static final long serialVersionUID = 1L;

	/*
	 * Read the redis lines for SYW events (non-Javadoc)
	 * 
	 * @see analytics.spout.RedisPubSubSpout#emit(java.lang.String)
	 */
	@Override
	protected void emit(String ret) {
		try {
			TimeUnit.MILLISECONDS.sleep(2);
		} catch (InterruptedException e) {
			LOGGER.error("Thread can not be interrupted",e);
		}
		if (ret != null) {
			_collector.emit(tuple("twitter",ret));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("source","message"));
	}

}
