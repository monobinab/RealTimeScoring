package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.concurrent.TimeUnit;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class TwitterRedisSpout extends RedisPubSubSpout {
	public TwitterRedisSpout(String host, int port, String pattern) {
		super(host, port, pattern);
	}

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
			e.printStackTrace();
		}
		if (ret != null) {
			_collector.emit(tuple("twitter",ret));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("source","message"));
	}

}
