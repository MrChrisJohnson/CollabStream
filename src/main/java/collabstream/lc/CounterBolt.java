package collabstream.lc;

import java.util.HashSet;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CounterBolt implements IRichBolt {
	private OutputCollector collector;
	private HashSet<Integer> lineNumSet = new HashSet<Integer>();
	
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
		System.out.println("######## CounterBolt.cleanup: counted " + lineNumSet.size() + " lines");
	}
	
	public void execute(Tuple tuple) {
		String line = tuple.getStringByField("line");
		Integer lineNum = tuple.getIntegerByField("lineNum");
		System.out.println("######## CounterBolt.execute: " + lineNum + ". " + line);
		lineNumSet.add(lineNum);
		collector.emit(tuple, new Values(lineNum));
		collector.ack(tuple);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lineNum"));
	}
}