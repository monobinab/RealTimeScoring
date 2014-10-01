package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.AAM_ATCTopology;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class FacebookRedisSpout extends RedisPubSubSpout {
	public FacebookRedisSpout(String host, int port, String pattern) {
		super(host, port, pattern);
	}

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(FacebookRedisSpout.class);
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
			LOGGER.warn("Thread is unable to sleep",e);
		}
		if (ret != null) {
			_collector.emit(tuple("facebook",ret));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("source","message"));
	}

}
