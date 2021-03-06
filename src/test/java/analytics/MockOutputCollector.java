package analytics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class MockOutputCollector extends OutputCollector {
	Map<String, List<Object>> tuple = new HashMap<String, List<Object>>();
	
	public Map<String, List<Object>> getTuple(){
		return tuple;
	}

	public MockOutputCollector(IOutputCollector delegate) {
		super(delegate);
		TestCaseListeners = new LinkedList<IBoltListener>();
	}

	private LinkedList<IBoltListener> TestCaseListeners;

	@Override
	public java.util.List<Integer> emit(java.util.List<Object> tuple) {
		Iterator<IBoltListener> iterator = TestCaseListeners.iterator();
		while (iterator.hasNext()) {
			iterator.next().onEmit(tuple);
		}
		//Create tuples here
		this.tuple.put("main", tuple);
		return null;
	};
	
	@Override
	public java.util.List<Integer> emit(String streamId, java.util.List<Object> tuple) {
		Iterator<IBoltListener> iterator = TestCaseListeners.iterator();
		while (iterator.hasNext()) {
			iterator.next().onEmit(tuple);
		}
		//Create tuples here
		//System.out.println(tuple);
		this.tuple.put(streamId, tuple);
		return null;
		
	}

	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors,
			List<Object> tuple) {

		Iterator<IBoltListener> iterator = TestCaseListeners.iterator();
		while (iterator.hasNext()) {
			iterator.next().onEmit(tuple);
		}
		return null;
	}

	public void addBoltListener(IBoltListener listener) {
		TestCaseListeners.add(listener);
	}

	@Override
	public void emitDirect(int taskId, String streamId,
			Collection<Tuple> anchors, List<Object> tuple) {
		// TODO Auto-generated method stub
	}

	@Override
	public void ack(Tuple input) {
		// TODO Auto-generated method stub
		Iterator<IBoltListener> iterator = TestCaseListeners.iterator();
		while (iterator.hasNext()) {
			iterator.next().onAck(input);
		}
	}

	@Override
	public void fail(Tuple input) {
		// TODO Auto-generated method stub
		Iterator<IBoltListener> iterator = TestCaseListeners.iterator();
		while (iterator.hasNext()) {
			iterator.next().onFail(input);
		}
	}

	@Override
	public void reportError(Throwable error) {
		// TODO Auto-generated method stub
		Iterator<IBoltListener> iterator = TestCaseListeners.iterator();
		while (iterator.hasNext()) {
			iterator.next().onError(error);
		}
	}

}
