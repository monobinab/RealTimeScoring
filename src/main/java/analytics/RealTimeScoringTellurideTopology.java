package analytics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import analytics.bolt.ScorePublishBolt;
import analytics.bolt.ScoringBolt;
import analytics.bolt.StrategyBolt;
import analytics.bolt.TellurideParsingBoltPOS;
import analytics.spout.WebsphereMQSpout;
import analytics.util.MQConnectionConfig;
import analytics.util.MqSender;
import analytics.util.WebsphereMQCredential;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.mongodb.DBObject;

/**
 * Created with IntelliJ IDEA. User: syermalk Date: 10/9/13 Time: 10:14 AM To
 * change this template use File | Settings | File Templates.
 */
public class RealTimeScoringTellurideTopology {

	static final Logger logger = Logger
			.getLogger(RealTimeScoringTellurideTopology.class);

	public static void main(String[] args) throws ConfigurationException {

		// Configure logger
		/*BasicConfigurator.configure();
		String log4jConfigFile = System.getProperty("user.dir")
				+ File.separator + "." + File.separator + "src"
				+ File.separator + "main" + File.separator + "resources"
				+ File.separator + "log4j.properties";
		PropertyConfigurator.configure(log4jConfigFile);*/
		creatLogger();
		MqSender.initJMS();
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
			@Override
			public List<Object> map(DBObject object) {
				if (object != null)
					logger.info(" in Mapper: " + object);
				List<Object> tuple = new ArrayList<Object>();
				tuple.add(object);
				return tuple;
			}

			@Override
			public String[] fields() {
				return new String[] { "document" };
			}
		};

		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential();

		topologyBuilder
				.setSpout(
						"npos1",
						new WebsphereMQSpout(mqCredential.getHostOneName(),
								mqCredential.getPort(), mqCredential
										.getQueueOneManager(), mqCredential
										.getQueueChannel(), mqCredential
										.getQueueName()), 1);
		topologyBuilder
				.setSpout(
						"npos2",
						new WebsphereMQSpout(mqCredential.getHostTwoName(),
								mqCredential.getPort(), mqCredential
										.getQueueTwoManager(), mqCredential
										.getQueueChannel(), mqCredential
										.getQueueName()), 1);

		// create definition of main spout for queue 1
		topologyBuilder.setBolt("parsing_bolt", new TellurideParsingBoltPOS())
				.shuffleGrouping("npos1").shuffleGrouping("npos2");
		topologyBuilder.setBolt("strategy_bolt", new StrategyBolt())
				.shuffleGrouping("parsing_bolt");
		topologyBuilder.setBolt("scoring_bolt", new ScoringBolt())
				.shuffleGrouping("strategy_bolt");
		topologyBuilder
				.setBolt(
						"ScorePublishBolt",	new ScorePublishBolt("rtsapp401p.prod.ch4.s.com", 6379,
								"score")).shuffleGrouping("scoring_bolt");
		// topologyBuilder.setBolt("map_bolt", new
		// RedisBolt("rtsapp302p.qa.ch3.s.com",
		// 6379,"sale_info")).shuffleGrouping("npos1").shuffleGrouping("npos2");

		Config conf = new Config();
		conf.setDebug(false);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("meetup_topology", conf,
					topologyBuilder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cluster.shutdown();

		}
	}

	private static void creatLogger() {
		// creates pattern layout
		PatternLayout layout = new PatternLayout();
		String conversionPattern = "%-7p %d [%t] %c %x - %m%n";
		layout.setConversionPattern(conversionPattern);

		// creates console appender
		ConsoleAppender consoleAppender = new ConsoleAppender();
		consoleAppender.setLayout(layout);
		consoleAppender.activateOptions();

		// creates file appender
		FileAppender fileAppender = new FileAppender();
		fileAppender.setFile(System.getProperty("user.dir") + File.separator
				+ "." + File.separator + "src" + File.separator + "main"
				+ File.separator + "resources" + File.separator + "RealTimeScoringLog.log");
		fileAppender.setLayout(layout);
		fileAppender.activateOptions();

		// configures the root logger
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.INFO);
		rootLogger.addAppender(consoleAppender);
		rootLogger.addAppender(fileAppender);

	}
}
