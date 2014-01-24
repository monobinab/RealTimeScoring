package metascale;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.DBObject;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import metascale.bolt.RedisBolt;
import metascale.spout.MeetupRsvpsSpout;
import metascale.spout.MongoCappedCollectionSpout;
import metascale.*;

/**
 * Created with IntelliJ IDEA.
 * User: syermalk
 * Date: 10/9/13
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class RealTimeScoringTopology {
    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
 
        MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
            @Override
            public List<Object> map(DBObject object) {
              List<Object> tuple = new ArrayList<Object>();
              // Add the a variable
              tuple.add(object.get("a"));
              // Return the mapped object
              return tuple;
            }

            @Override
            public String[] fields() {
              return new String[]{"a"};
            }

          };

          topologyBuilder.setSpout("mongodb", new MongoCappedCollectionSpout("mongodb://127.0.0.1:27017/storm_mongospout_test", "aggregation", mongoMapper), 1);

        // create definition of main spout for queue 1
        topologyBuilder.setBolt("redis_bolt", new RedisBolt()).shuffleGrouping("meetup_rsvp_spout");
        Config conf = new Config();
        conf.setDebug(false);

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("meetup_topology", conf, topologyBuilder.createTopology());
        }
    }

}


