package analytics;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.mongodb.DBObject;
import analytics.bolt.*;
import analytics.spout.WebsphereMQSpout;

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

          topologyBuilder.setSpout("npos1", new WebsphereMQSpout("iasapp304p.prod.ch3.s.com", 1414, "SQLP0393", "MARKETAN.SVRCONN", "STORM.NPOS.MASCORED.QC01"), 1);
        topologyBuilder.setSpout("npos2", new WebsphereMQSpout("iasapp305p.prod.ch3.s.com", 1414, "SQLP0394", "MARKETAN.SVRCONN", "STORM.NPOS.MASCORED.QC01"), 1);

        // create definition of main spout for queue 1
        //topologyBuilder.setBolt("scoring_bolt", new ScoringBolt()).shuffleGrouping("npos1").shuffleGrouping("npos2");
        topologyBuilder.setBolt("map_bolt", new RedisBolt("rtsapp401p.prod.ch4.s.com", 6379,"sale_info"),5).shuffleGrouping("npos1").shuffleGrouping("npos2");

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


