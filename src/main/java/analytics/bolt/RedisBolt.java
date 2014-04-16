/**
 * 
 */
package analytics.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ibm.jms.JMSMessage;
import analytics.util.StoreZipMap;
import redis.clients.jedis.Jedis;
import shc.npos.segments.Segment;
import shc.npos.util.SegmentUtils;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.Collection;
import java.util.Map;

public class RedisBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private Jedis jedis;
    private OutputCollector outputCollector;
    final String host;
    final int port;
    final String pattern;


    public RedisBolt(String host, int port, String pattern) {
        this.host = host;
        this.port = port;
        this.pattern = pattern;
    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        jedis = new Jedis(host, port);
        this.outputCollector = collector;
    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {

        JMSMessage document = (JMSMessage) input.getValueByField("npos");

        try {
            String nposTransaction = ((TextMessage) document).getText();
            //System.out.println(nposTransaction);
            Collection<Segment> saleSegments = SegmentUtils.findAllSegments(nposTransaction, "B1");
            Integer zip = 0;
            char sywrCardUsed = 'N';
            String amount = "0";
            for (Segment segment : saleSegments) {
                String transactionType = segment.getSegmentBody().get("Transaction Type Code");
                if ("1".equals(transactionType)) {
                    String store = segment.getSegmentBody().get("Store Number");
                    zip = StoreZipMap.getInstance().getZip(store);
                }
                amount = segment.getSegmentBody().get("Transaction Total");
            }

            Collection<Segment> b2Segments = SegmentUtils.findAllSegments(nposTransaction, "B2");
            for (Segment segment : b2Segments) {
                if (segment != null && segment.getSegmentDescription() != null && segment.getSegmentDescription().contains("Type 8")) {
                    sywrCardUsed = 'Y';
                }
            }

            StringBuffer saleInfo = new StringBuffer().append(zip).append(':').append(sywrCardUsed).append(':').append(amount);

            if (zip != null && zip != 0)
                jedis.publish(pattern, saleInfo.toString());
        } catch (JMSException e) {
            e.printStackTrace();
        }
        //jedis.hincrBy("meetup_country", country.toString(), 1);


	}

    /*
      * (non-Javadoc)
      *
      * @see
      * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
      * topology.OutputFieldsDeclarer)
      */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
