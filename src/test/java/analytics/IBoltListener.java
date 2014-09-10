package analytics;

import java.util.List;

import backtype.storm.tuple.Tuple;

public interface IBoltListener {
	public void onAck(Tuple input);
public void onFail(Tuple input);
public void onError(Throwable error);
public void onEmit(List<Object> tuple);
}
