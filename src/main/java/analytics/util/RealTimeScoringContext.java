package analytics.util;

import java.util.Map;

/**
 * Created by syermalk on 4/1/14.
 */
public class RealTimeScoringContext {

    private Object previousValue;

    /* need no-argument constructor */
    public RealTimeScoringContext() {
    }

    private Double amount; // Map should contain variable as key and amount as value

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double a) {
        this.amount = a;
    }

    public Object getPreviousValue() {
        return previousValue;
    }

    public void setPreviousValue(Object object) {
        this.previousValue = object;
    }

}
