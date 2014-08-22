package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyCountTransactions implements Strategy {

	private int daysToExpiration = 2;
	

    @Override
    public Change execute(RealTimeScoringContext context) {
    	if(context.getPreviousValue() == null || !(context.getPreviousValue() instanceof Integer)) {
    		return new Change(1, calculateExpirationDate());
    	}
		return new Change(new Integer(context.getPreviousValue().toString()) + 1, calculateExpirationDate());
    }
	
    private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}
}
