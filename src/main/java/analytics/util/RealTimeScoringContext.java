package analytics.util;

/**
 * Created by syermalk on 4/1/14.
 */
public class RealTimeScoringContext {

    private Object previousValue;

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

    public Object getPreviousValue() {
        return previousValue;
    }

    public void setPreviousValue(Object object) {
        this.previousValue = object;
    }

}
