package analytics.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class PrinterBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    System.out.println("in printer bolt " + tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
