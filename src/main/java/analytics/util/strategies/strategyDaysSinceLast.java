package analytics.util.strategies;

import analytics.util.RealTimeScoringContext;

public class strategyDaysSinceLast implements Strategy {

	@Override
    public Object execute(RealTimeScoringContext context) {
		return 1;
	}
}
