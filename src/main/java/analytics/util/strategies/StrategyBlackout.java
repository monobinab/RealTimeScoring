package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyBlackout implements Strategy {

	private int daysToExpiration = 730;
	private Object value = 1;
	@Override
	public Change executeBlackout(RealTimeScoringContext context, Date transactionDate) {
		return new Change(this.value, calculateBlackoutExpirationDate(transactionDate), transactionDate);
	}
    @Override
	public Change execute(RealTimeScoringContext context) {
		return new Change(this.value, calculateExpirationDate());
	}
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
	private Date calculateBlackoutExpirationDate(Date transactionDate) {
		return new LocalDate(transactionDate).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
}
