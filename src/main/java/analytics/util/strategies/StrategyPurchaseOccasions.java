package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyPurchaseOccasions implements Strategy {
	private int daysToExpiration = 365;

	@Override
	public Change execute(RealTimeScoringContext context) {
		return new Change(context.getValue(), calculateExpirationDate());
	}
	
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}

}
