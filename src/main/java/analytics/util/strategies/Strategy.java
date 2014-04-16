package analytics.util.strategies;

import analytics.util.Change;
import analytics.util.RealTimeScoringContext;

/**
 * Created by syermalk on 4/1/14.
 */
public interface Strategy {
    Change execute(RealTimeScoringContext context);
}
