package analytics.spout;


import java.util.concurrent.TimeUnit;

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
    	try {
			TimeUnit.MILLISECONDS.sleep(2);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	if (ret != null )
        {
            String tokens[]= StringUtils.split(ret, ",");
            if (tokens.length >= 2)
            {
                //System.out.println(tokens[1]);
            	_collector.emit(tuple(tokens[1], ret));
            	
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uuid","message"));
    }

}
