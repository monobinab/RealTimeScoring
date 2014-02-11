package metascale;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.mongodb.DBObject;
import metascale.bolt.ScoringBolt;
import metascale.spout.MongoCappedCollectionSpout;

import java.util.ArrayList;
import java.util.List;

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
                if (object != null) System.out.println(" in Mapper: " + object);
                List<Object> tuple = new ArrayList<Object>();
                tuple.add(object);
                return tuple;
            }

            @Override
            public String[] fields() {
                return new String[]{"document"};
            }
        };

          topologyBuilder.setSpout("mongodb", new MongoCappedCollectionSpout("mongodb://151.149.191.228:27017/test", "BrightTagFeed", mongoMapper), 1);

        // create definition of main spout for queue 1
        topologyBuilder.setBolt("scoring_bolt", new ScoringBolt()).shuffleGrouping("mongodb");
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


