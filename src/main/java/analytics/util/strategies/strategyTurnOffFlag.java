package analytics.util.strategies;

import analytics.util.RealTimeScoringContext;

public class strategyTurnOffFlag implements Strategy {
	
    @Override
    public Object execute(RealTimeScoringContext context) {
        return 0;
    }
}
