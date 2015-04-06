package analytics.spout;


import java.util.concurrent.TimeUnit;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static backtype.storm.utils.Utils.tuple;

public class AAMRedisPubSubSpout extends RedisPubSubSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AAMRedisPubSubSpout.class);
	public AAMRedisPubSubSpout(int number, String pattern, String systemProperty) {
		super(number, pattern, systemProperty)	;
		}
    @Override
    protected void emit(String ret) {
    	LOGGER.debug("Reading message from Redis");
    	try {
			TimeUnit.MILLISECONDS.sleep(2);
		} catch (InterruptedException e) {
			LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
		}
    	if (ret != null )
        {
            _collector.emit(tuple(ret));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

}
