package analytics.util.objects;

import java.util.Map;

/**
 * Created by syermalk on 4/1/14.
 */
public class RealTimeScoringContext {

    private Object previousValue;
    private Object value; // Map should contain variable as key and amount as value

    /* need no-argument constructor */
    public RealTimeScoringContext() {
    }


    public Object getValue() {
        return value;
    }

    public void setValue(Object a) {
        this.value = a;
    }

    public Object getPreviousValue() {
        return previousValue;
    }

    public void setPreviousValue(Object object) {
        this.previousValue = object;
    }

}
