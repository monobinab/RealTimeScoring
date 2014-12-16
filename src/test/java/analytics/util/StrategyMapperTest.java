package analytics.util;

import org.junit.Test;

import junit.framework.Assert;
import analytics.util.objects.StrategyMapper;
import analytics.util.strategies.Strategy;
import analytics.util.strategies.StrategyBoostProductTotalCount;
import analytics.util.strategies.StrategyCountTraitDates;
import analytics.util.strategies.StrategyCountTraits;
import analytics.util.strategies.StrategyCountTransactions;
import analytics.util.strategies.StrategyDaysSinceLast;
import analytics.util.strategies.StrategyTurnOnFlag;

public class StrategyMapperTest {
	@Test
	public void testGetStrategyCountTransactions() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyCountTransactions");
		Assert.assertTrue(s instanceof StrategyCountTransactions);
	}
	@Test
	public void testGetStrategyCountTraitDates() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyCountTraitDates");
		Assert.assertTrue(s instanceof StrategyCountTraitDates);
	}
	@Test
	public void testGetStrategyCountTraits() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyCountTraits");
		Assert.assertTrue(s instanceof StrategyCountTraits);
	}
	@Test
	public void testGetStrategyDaysSinceLast() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyDaysSinceLast");
		Assert.assertTrue(s instanceof StrategyDaysSinceLast);
	}
	@Test
	public void testGetStrategyTurnOnFlag() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyTurnOnFlag");
		Assert.assertTrue(s instanceof StrategyTurnOnFlag);
	}
	@Test
	public void testGetStrategyBoostProductTotalCount() {
		Strategy s = StrategyMapper.getInstance().getStrategy("StrategyBoostProductTotalCount");
		Assert.assertTrue(s instanceof StrategyBoostProductTotalCount);
	}
}
