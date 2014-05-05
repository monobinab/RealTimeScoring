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
import java.util.Arrays;
import java.util.Collection;
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
    private Collection<String> columns;



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
        homesListedCollection = db.getCollection("homes");
        //modelCollection = db.getCollection("modelVariables");
        //jedis = new Jedis("151.149.116.48");

        columns = Arrays.asList(new String[]{"street", "city", "county", "state", "zip","saleStatus", "date", "bed", "bath", "area", "price"});

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
        9 "lotSize"
        10 "price"
         */
        //List values = input.getValues();
        //System.out.println (values.get(0));
        BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
        int count = 0;

        for(String column:columns)
        {
           builder.append(column,input.getStringByField(column));
        }


        BasicDBObject queryMbr = new BasicDBObject("street", input.getStringByField("street"));
        queryMbr.append("city", input.getStringByField("city"));
        queryMbr.append("state", input.getStringByField("state"));

        DBObject old = homesListedCollection.findOne(queryMbr);
        if(old != null)
        {
            //flip the flag if necessary
            if (!input.getStringByField("saleStatus").equals(old.get("saleStatus"))){
                builder.append("saleStatus",input.getStringByField("saleStatus"));
                homesListedCollection.update(queryMbr, builder.get());
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
