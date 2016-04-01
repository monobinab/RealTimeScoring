package analytics.util.strategies;

import java.util.Date;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

/**
 * Created by syermalk on 4/1/14.
 */
public interface Strategy {
    Change execute(RealTimeScoringContext context);

	Change executeBlackout(RealTimeScoringContext context, Date transactionDate);
}
