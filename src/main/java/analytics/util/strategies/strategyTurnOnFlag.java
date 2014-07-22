package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.Change;
import analytics.util.RealTimeScoringContext;

public class strategyTurnOnFlag implements Strategy {
	
	private int daysToExpiration = 2;
	private Object value = 1;

    @Override
    public Change execute(RealTimeScoringContext context) {
		return new Change(this.value, calculateExpirationDate());
	}
	
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
}