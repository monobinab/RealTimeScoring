package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyDaysSinceLast implements Strategy {

	private int daysToExpiration = 2;
	private Object value = 1;
	
	//Change(String hashed, String varName, String strat, Object val, Date expDate)
	
	@Override
    public Change execute(RealTimeScoringContext context) {
		return new Change(this.value, calculateExpirationDate());
	}
	
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
		
}
