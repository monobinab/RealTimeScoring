package analytics.bolt;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ParsingBoltWebTraitsTest {
	
	ParsingBoltWebTraits testTarget =  new ParsingBoltWebTraits();
	String testVal;
	String [] result;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void splitRecTestNullString() {
		// Case 0
		testVal = null;
		result = testTarget.splitRec(testVal);
		assertNull("Case 0: Should have returned null.", result);
	}
	
	@Test
	public void splitRecTestEmptyString(){
		//Case 1
		testVal = "";
		result = testTarget.splitRec(testVal);
		assertNull("Case 1: Should have returned null.", result);
	}
	
	
	@Test
	public void splitRectTestCase2(){
		//Case 2
		testVal = ",,,,,,,,,,,,,,,,,,,,,,,,,,,";
		result = testTarget.splitRec(testVal);
		assertNull("Case 2: Should have returned null.", result);
	}
	
	/*	//Case 3
    @Test
    public void splitRectTestCase3(){
    	testVal = "58452933496186656922631844021025043285,['206658','270775']";
		result = testTarget.splitRec(testVal);
		assertEquals(result[0], "58452933496186656922631844021025043285");
		assertEquals(result[1], "206658");
		assertEquals(result[2], "270775");
    }
		//Case 4 with space
	  @Test
	    public void splitRectTestCase4(){
	    	testVal = "58452933496186656922631844021025043285, ['206658', '270775']";
			result = testTarget.splitRec(testVal);
			assertEquals(result[0], "58452933496186656922631844021025043285");
			assertEquals(result[1], "206658");
			assertEquals(result[2], "270775");
    }*/
	

}
