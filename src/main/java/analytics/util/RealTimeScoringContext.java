package analytics.util;

/**
 * Created by syermalk on 4/1/14.
 */
public class RealTimeScoringContext {

    private double previousValue;

    /* need no-argument constructor */
    public RealTimeScoringContext() {
    }

    private TransactionLineItem transactionLineItem;

    public TransactionLineItem getTransactionLineItem() {
        return transactionLineItem;
    }

    public void setTransactionLineItem(TransactionLineItem transactionLineItem) {
        this.transactionLineItem = transactionLineItem;
    }

    public double getPreviousValue() {
        return previousValue;
    }

    public void setPreviousValue(double previousValue) {
        this.previousValue = previousValue;
    }

}
