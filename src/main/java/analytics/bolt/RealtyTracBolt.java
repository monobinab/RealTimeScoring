/**
 * 
 */
package analytics.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public class RealtyTracBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private OutputCollector outputCollector;

    DB db;
    MongoClient mongoClient;
    DBCollection homesListedCollection;




    public void setOutputCollector(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    /*
         * (non-Javadoc)
         *
         * @see backtype.storm.task.IBolt#prepare(java.util.Map,
         * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
         */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

	/*
	 * (non-Javadoc)
	 *
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */

        try {
            mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        db = mongoClient.getDB("RealTimeScoring");
        //db.authenticate(configuration.getString("mongo.db.user"), configuration.getString("mongo.db.password").toCharArray());
	    db.authenticate("rtsw", "5core123".toCharArray());
        homesListedCollection = db.getCollection("homesListed");
        //modelCollection = db.getCollection("modelVariables");
        //jedis = new Jedis("151.149.116.48");

    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {

        /*
        0 "street",
        1 "city",
        2 "county",
        3 "state",
        4 "saleStatus",
        5 "date",
        6 "bed",
        7 "bath",
        8 "area",
        9 "price"
         */
        List values = input.getValues();
        System.out.println (values.get(0));
        BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
        int count = 0;
        for (Object value:values)
        {
           builder.append(String.valueOf(count++), value);
        }

        BasicDBObject queryMbr = new BasicDBObject("0", values.get(0));
        queryMbr.append("1", values.get(1));
        queryMbr.append("3", values.get(3));

        DBObject old = homesListedCollection.findOne(queryMbr);
        if(old != null)
        {
            //flip the flag if necessary
            if (!values.get(4).toString().equals(old.get("4"))){
                builder.append("4",values.get(4).toString());
                homesListedCollection.update(old, builder.get());
            }
        }
        else
        {
            homesListedCollection.insert(builder.get());
        }


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
