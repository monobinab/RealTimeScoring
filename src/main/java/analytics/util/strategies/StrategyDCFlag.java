package analytics.util.strategies;

import java.util.Date;

import org.joda.time.LocalDate;

import analytics.util.objects.Change;
import analytics.util.objects.RealTimeScoringContext;

public class StrategyDCFlag implements Strategy {

	private int daysToExpiration = 30;
	private int flag = 0;

	@Override
	public Change execute(RealTimeScoringContext context) {
		// return new Change(new Double(context.getPreviousValue().toString()) +
		// context.getValue(), calculateExpirationDate());
		Object contextPrevVal = context.getPreviousValue();
		if (contextPrevVal != null) {
			if (contextPrevVal instanceof String) {
				try {
					contextPrevVal = Double.parseDouble((String) contextPrevVal);
				} catch (NumberFormatException e) {
					try {
						contextPrevVal = Integer.parseInt((String) contextPrevVal);
					} catch (NumberFormatException e1) {
					}
				}
			}
			if ((contextPrevVal instanceof Double) && Double.valueOf(contextPrevVal.toString()) > 0) {
				this.flag = 1;
			} else if ((contextPrevVal instanceof Integer) && Integer.valueOf(contextPrevVal.toString()) > 0) {
				this.flag = 1;
			}
		}

		return new Change((Object) this.flag, calculateExpirationDate());
	}

	private Date calculateExpirationDate() {
		return new LocalDate(new Date()).plusDays(this.daysToExpiration).toDateMidnight().toDate();
	}

}
