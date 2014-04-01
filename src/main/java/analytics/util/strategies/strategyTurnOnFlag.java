package analytics.util.strategies;

import analytics.util.RealTimeScoringContext;

public class strategyTurnOnFlag implements Strategy {

    @Override
    public Object execute(RealTimeScoringContext context) {
        return 1;
    }
}
