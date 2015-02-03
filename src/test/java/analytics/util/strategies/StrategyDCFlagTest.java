package analytics.util.strategies;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyDCFlagTest {
	StrategyDCFlag dcStrategy;
	
	@Before
	public void setUp() throws Exception {
		dcStrategy = new StrategyDCFlag();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void sumIsNegative() {
		RealTimeScoringContext context =  new RealTimeScoringContext();
		context.setValue("{2015-02-03=7.0, 2015-02-04=-8.0, 2015-02-05=-5.0}");
		Change change = dcStrategy.execute(context);
		int val = (Integer) change.getValue();
		assertEquals(0,val);
	}
	
	@Test
	public void sumIsPositive(){
		RealTimeScoringContext context =  new RealTimeScoringContext();
		context.setValue("{2015-02-03=7.0, 2015-02-04=8.0, 2015-02-05=5.0}");
		Change change = dcStrategy.execute(context);
		int val = (Integer) change.getValue();
		assertEquals(1, val);
	}
	
	@Test
	public void sumIsZero(){
		RealTimeScoringContext context =  new RealTimeScoringContext();
		context.setValue("{2015-02-03=7.0, 2015-02-04=8.0, 2015-02-05=-15.0}");
		Change change = dcStrategy.execute(context);
		int val = (Integer) change.getValue();
		assertEquals(0, val);
	}
	
	@Test
	public void emptyMap(){
		RealTimeScoringContext context =  new RealTimeScoringContext();
		context.setValue("{}");
		Change change = dcStrategy.execute(context);
		int val = (Integer) change.getValue();
		assertEquals(0, val);
	}
	
	@Test
	public void invalidValues1(){
		RealTimeScoringContext context =  new RealTimeScoringContext();
		context.setValue("{2015-02-03=s, 2015-02-04=4.0}");
		Change change = dcStrategy.execute(context);
		int val = (Integer) change.getValue();
		assertEquals(1, val);
	}
	
	@Test
	public void invalidValues2(){
		RealTimeScoringContext context =  new RealTimeScoringContext();
		context.setValue("{2015-02-03=s, asas=asdsad, 2015-02-04=4.0}");
		Change change = dcStrategy.execute(context);
		int val = (Integer) change.getValue();
		assertEquals(1, val);
	}

}
