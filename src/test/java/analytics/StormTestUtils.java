package analytics;


import static org.mockito.Mockito.*;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StormTestUtils {
	 public static Tuple mockTuple(String message, String source) {
	        Tuple tuple = mock(Tuple.class);
	        when(tuple.getValueByField("message")).thenReturn(message);	       
	        when(tuple.getValueByField("source")).thenReturn(source);
	        when(tuple.getString(0)).thenReturn(message);
	        when(tuple.getValues()).thenReturn(new Values(message));
	        when(tuple.getSourceComponent()).thenReturn("abc");//Testing
	
	        return tuple;
	    } 
}
