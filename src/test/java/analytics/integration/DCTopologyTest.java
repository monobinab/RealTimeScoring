package analytics.integration;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DCTopologyTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	//TODO:
	//Key here to test parsing bolt is to see if the bolt emitted
	//in order to do that, we need to be able to set collector to mock collector 
	//then check collector if thing is emitted and assert it
	//we  need different messages to pass into the fake spout
	//message with questionid answerid promptgroupname memberid
	//with or without strength and varname
	//with all of the above -> emit is true
	// else emit is false
	
	//TODO:
	//Testing for SYW Topology
	//

}
