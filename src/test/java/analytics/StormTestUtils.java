package analytics;


import static org.mockito.Mockito.*;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StormTestUtils {
	 public static Tuple mockTuple(String message, String source) {
	        Tuple tuple = mock(Tuple.class);
	        when(tuple.getStringByField("message")).thenReturn(message);
	        when(tuple.getValueByField("message")).thenReturn(message);	  
	        when(tuple.getString(0)).thenReturn(message);
	        when(tuple.getValues()).thenReturn(new Values(message));
	        
	        when(tuple.getValueByField("source")).thenReturn(source);
	        return tuple;
	    } 
	 public static Tuple mockInteractionTuple(String lId, String message, String interactionType) {
		 Tuple tuple = mock(Tuple.class);
		 when(tuple.getStringByField("l_id")).thenReturn(lId);
		 when(tuple.getValueByField("l_id")).thenReturn(lId);
		 when(tuple.getStringByField("message")).thenReturn(message);
		 when(tuple.getStringByField("InteractionType")).thenReturn(interactionType);
		 return tuple;
	 }
	 public static Tuple mockMemberTuple(String lId, String source){
		 Tuple tuple = mock(Tuple.class);
		 when(tuple.getStringByField("l_id")).thenReturn(lId);
		 when(tuple.getStringByField("source")).thenReturn(source);
		 return tuple;
	 }
	 
}
