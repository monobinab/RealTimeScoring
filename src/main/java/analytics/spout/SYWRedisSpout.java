package analytics.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

import clojure.main;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import analytics.util.objects.SYWInteraction;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class SYWRedisSpout extends RedisPubSubSpout {
	public SYWRedisSpout(String host, int port, String pattern) {
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
		// TODO: Find why there was a sleep?? Copied form AAMTopology
		try {
			TimeUnit.MILLISECONDS.sleep(2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (ret != null) {
			_collector.emit(tuple(ret));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
