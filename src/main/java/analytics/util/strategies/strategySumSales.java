package analytics.util.strategies;

import analytics.util.RealTimeScoringContext;

public class strategySumSales implements Strategy {


    @Override
    public Object execute(RealTimeScoringContext context) {
        return context.getPreviousValue()+context.getTransactionLineItem().getAmount();
    }
}
