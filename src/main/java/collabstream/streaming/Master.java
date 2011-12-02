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

public class Master implements IRichBolt {
	public static final int TO_WORKER_STREAM_ID = 1;
	public static final int TO_MATRIX_STORE_STREAM_ID = 2;
	
	private OutputCollector collector;
	private final Configuration config;
	private int count = 0;
	
	public Master(Configuration config) {
		this.config = config;
	}
	
	public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		MsgType msgType = (MsgType)tuple.getValue(0);
		if (config.debug && msgType != PROCESS_BLOCK_FIN) {
			System.out.println("######## Master.execute: " + msgType + " " + tuple.getValue(1));
		}
		BlockPair bp;
		
		switch (msgType) {
		case TRAINING_EXAMPLE:
			TrainingExample ex = (TrainingExample)tuple.getValue(1);
			collector.ack(tuple);
			bp = new BlockPair(config.getUserBlockIdx(ex.userId), config.getItemBlockIdx(ex.itemId));
			collector.emit(TO_WORKER_STREAM_ID, new Values(TRAINING_EXAMPLE, ex, null, bp.userBlockIdx));
			if (++count == 2) {
				collector.emit(TO_WORKER_STREAM_ID, new Values(PROCESS_BLOCK_REQ, null, new BlockPair(2,4), 2));
			}
			break;
		case PROCESS_BLOCK_FIN:
			bp = (BlockPair)tuple.getValue(1);
			TrainingExample latest = (TrainingExample)tuple.getValue(2);
			if (config.debug) {
				System.out.println("######## Master.execute: " + msgType + " " + bp + " " + latest);
			}
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Field "userBlockIdx" used solely for grouping
		declarer.declareStream(TO_WORKER_STREAM_ID, new Fields("msgType", "example", "blockPair", "userBlockIdx"));
		declarer.declareStream(TO_MATRIX_STORE_STREAM_ID, new Fields("msgType"));
	}
}