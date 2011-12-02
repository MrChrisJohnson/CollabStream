package collabstream.streaming;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import static collabstream.streaming.MsgType.*;

public class RatingsSource implements IRichSpout {
	protected SpoutOutputCollector collector;
	private TrainingExample[] example = { new TrainingExample(0,13,21,17.0f), new TrainingExample(1,16,23,17.0f) };
	private int curr = 0;
	
	public boolean isDistributed() {
		return false;
	}
	
	public void open(Map stormConfig, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	public void close() {
	}
	
	public void ack(Object msgId) {
	}
	
	public void fail(Object msgId) {
		System.err.println("######## RatingsSource.fail: " + msgId);
	}
	
	public void nextTuple() {
		if (curr < 2) {
			TrainingExample ex = example[curr++];
			collector.emit(new Values(TRAINING_EXAMPLE, ex), ex);
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msgType", "example"));
	}
}