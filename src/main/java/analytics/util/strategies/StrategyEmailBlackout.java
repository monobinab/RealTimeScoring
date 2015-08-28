package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyEmailBlackout implements Strategy{
	private int daysToExpiration = 30;
	private Object value = 1;

	@Override
	public Change execute(RealTimeScoringContext context) {		
		return new Change(context.getValue(), calculateExpirationDate());
	}
	
	
	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
	

}