package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyPurchase implements Strategy {
	private int daysToExpiration = 30;
	private Object value = 0;

    @Override
	public Change execute(RealTimeScoringContext context) {
		return new Change(this.value, calculateExpirationDate());
	}
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
	@Override
	public Change executeBlackout(RealTimeScoringContext context, Date transactionDate) {
		return null;
	}
}
