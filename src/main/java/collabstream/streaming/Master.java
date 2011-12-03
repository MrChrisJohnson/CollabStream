package collabstream.streaming;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import static collabstream.streaming.MsgType.*;

public class Master implements IRichBolt {
	private OutputCollector collector;
	private final Configuration config;
	private BlockPair[][] blockPair;
	private TrainingExample[][] latestExample;
	private Set<BlockPair> freeSet = new HashSet<BlockPair>();
	
	public Master(Configuration config) {
		this.config = config;		
		blockPair = new BlockPair[config.numUserBlocks][config.numItemBlocks];
		latestExample = new TrainingExample[config.numUserBlocks][config.numItemBlocks];
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
			int userBlockIdx = config.getUserBlockIdx(ex.userId);
			int itemBlockIdx = config.getItemBlockIdx(ex.itemId);
			latestExample[userBlockIdx][itemBlockIdx] = ex;
			
			bp = blockPair[userBlockIdx][itemBlockIdx];
			if (bp == null) {
				bp = blockPair[userBlockIdx][itemBlockIdx] = new BlockPair(userBlockIdx, itemBlockIdx);
				freeSet.add(bp);
			}
			
			collector.ack(tuple);
			collector.emit(new Values(TRAINING_EXAMPLE, null, ex, userBlockIdx));
			
			if (ex.timestamp % 2 == 1) {
				collector.emit(new Values(PROCESS_BLOCK_REQ, bp, null, bp.userBlockIdx));
			}
			break;
		case PROCESS_BLOCK_FIN:
			bp = (BlockPair)tuple.getValue(1);
			TrainingExample latest = (TrainingExample)tuple.getValue(2);
			if (config.debug) {
				System.out.println("######## Master.execute: " + msgType + " " + bp + " " + latest);
			}
			collector.emit(new Values(USER_BLOCK_REQ, bp, null, bp.userBlockIdx));
			collector.emit(new Values(ITEM_BLOCK_REQ, bp, null, bp.userBlockIdx));
			break;
		case USER_BLOCK:
			bp = (BlockPair)tuple.getValue(1);
			System.out.println("######## Master.execute: userBlock=\n" + MatrixUtils.toString((float[][])tuple.getValue(2)));
			break;
		case ITEM_BLOCK:
			bp = (BlockPair)tuple.getValue(1);
			System.out.println("######## Master.execute: itemBlock=\n" + MatrixUtils.toString((float[][])tuple.getValue(2)));
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Field "userBlockIdx" used solely for grouping
		declarer.declare(new Fields("msgType", "blockPair", "example", "userBlockIdx"));
	}
}