package collabstream.streaming;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MatrixStore implements IRichBolt {
	private OutputCollector collector;
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		System.out.println("######## DataStore.execute: \n" + MatrixUtils.toString((float[][])tuple.getValue(0)));
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}