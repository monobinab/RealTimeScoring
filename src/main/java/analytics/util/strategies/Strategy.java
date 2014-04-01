package analytics.util.strategies;

import analytics.util.RealTimeScoringContext;

/**
 * Created by syermalk on 4/1/14.
 */
public interface Strategy {
    Object execute(RealTimeScoringContext context);
}
