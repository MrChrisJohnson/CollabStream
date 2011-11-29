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
	private TrainingExample[] example = {
		new TrainingExample(1,1,3.14f), new TrainingExample(2,1,2.71828f), new TrainingExample(3,1,9000f)
	};
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
	}
	
	public void nextTuple() {
		if (curr < 3) {
			collector.emit(new Values(TRAINING_EXAMPLE, example[curr++]));
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msgType", "msg"));
	}
}