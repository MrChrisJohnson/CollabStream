package collabstream.streaming;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import static collabstream.streaming.MsgType.*;

public class Worker implements IRichBolt {
	private OutputCollector collector;
	long start;
	
	public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		MsgType msgType = (MsgType)tuple.getValue(0);
		switch (msgType) {
		case PROCESS_BLOCK_REQ:
			System.out.println("######## Worker.execute: " + tuple.getValue(1));
			start = System.currentTimeMillis();
			collector.emit(new Values(USER_BLOCK_REQ));
			break;
		case USER_BLOCK:
			long end = System.currentTimeMillis();
			float[][] block = (float[][])tuple.getValue(1);
			int numRows = block.length;
			int numCols = block[0].length;
			System.out.printf(
				"######## Worker.execute: %dx%d %f %d\n", numRows, numCols, block[numRows-1][numCols-1], end - start);
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msgType"));
	}
}