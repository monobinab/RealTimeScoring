package analytics;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import analytics.bolt.ParsingBoltDCTest;
import analytics.bolt.PersistDCBoltTest;
import analytics.integration.DCTopologyTest;
import analytics.util.strategies.StrategyDCFlagTest;

@RunWith(Suite.class)
@SuiteClasses({ ParsingBoltDCTest.class, PersistDCBoltTest.class, DCTopologyTest.class, StrategyDCFlagTest.class})
public class AllDCTests {
	//To see test coverage for DC, install Eclemma from eclipse marketplace,
	//run this suite with "coverage as".
	
	//Files needs to be covered:
	//DCTopology.java
	//ParsingBoltDC.java
	//PersistDCBolt.java
	//DCParserHandler.java
	//StrategyDCFlag.java
	//DCDao.java
	//MemberDCDao.java
	//Two methods in JsonUtils.java
}
