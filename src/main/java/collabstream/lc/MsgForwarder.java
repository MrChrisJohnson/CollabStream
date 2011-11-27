package collabstream.lc;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MsgForwarder implements IRichBolt {
	private OutputCollector collector;
	public int fileReaderId, counterId;
	
	public MsgForwarder(int fileReaderId, int counterId) {
		this.fileReaderId = fileReaderId;
		this.counterId = counterId;
	}
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		// The ID of the component that emitted the tuple is mapped directly to the ID of the forwarding stream.
		collector.emit(tuple.getSourceComponent(), tuple.getValues());
		collector.ack(tuple);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(fileReaderId, new Fields("lineNum", "line"));
		declarer.declareStream(counterId, new Fields("lineNum"));
	}
}