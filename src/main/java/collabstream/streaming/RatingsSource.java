package collabstream.streaming;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class RatingsSource implements IRichSpout {
	protected SpoutOutputCollector collector;
	
	public boolean isDistributed() {
		return false;
	}
	
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	public void close() {
	}
	
	public void ack(Object msgId) {
	}
	
	public void fail(Object msgId) {
	}
	
	public void nextTuple() {
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}