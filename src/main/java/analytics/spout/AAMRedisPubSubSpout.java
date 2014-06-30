package analytics.spout;


import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import static backtype.storm.utils.Utils.tuple;

public class AAMRedisPubSubSpout extends RedisPubSubSpout {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger LOG = Logger.getLogger(AAMRedisPubSubSpout.class);



    public AAMRedisPubSubSpout(String host, int port, String pattern) {
        super(host, port, pattern);
    }

    @Override
    protected void emit(String ret) {
        if (ret != null )
        {
            String tokens[]= StringUtils.split(ret, ",");
            if (tokens.length >= 2)
            {
                _collector.emit(tuple(tokens[1], ret));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uuid","message"));
    }

}
