package collabstream.streaming;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import static collabstream.streaming.MsgType.*;

public class MatrixStore implements IRichBolt {
	private OutputCollector collector;
	private final Configuration config;
	private final Map<Integer, float[][]> userBlockMap = new ConcurrentHashMap<Integer, float[][]>();
	private final Map<Integer, float[][]> itemBlockMap = new ConcurrentHashMap<Integer, float[][]>();
	
	public MatrixStore(Configuration config) {
		this.config = config;
	}
	
	public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		MsgType msgType = (MsgType)tuple.getValue(0);
		BlockPair bp = (BlockPair)tuple.getValue(1);
		Integer taskIdObj = (Integer)tuple.getValue(3);
		int taskId = (taskIdObj != null) ? taskIdObj.intValue() : tuple.getSourceTask();
		if (config.debug) {
			System.out.println("######## MatrixStore.execute: " + msgType + " " + bp + " [" + taskId + "]");
		}
		
		switch (msgType) {
		case USER_BLOCK_REQ:
			// In general, this pattern of access is not thread-safe. But since requests with the same userBlockIdx
			// are sent to the same thread, it should be safe in our case.
			float[][] userBlock = userBlockMap.get(bp.userBlockIdx);
			if (userBlock == null) {
				userBlock = MatrixUtils.generateRandomMatrix(config.getUserBlockLength(bp.userBlockIdx), config.numLatent);
				userBlockMap.put(bp.userBlockIdx, userBlock);
			}
			collector.emitDirect(taskId, new Values(USER_BLOCK, bp, (Object)userBlock));
			break;
		case ITEM_BLOCK_REQ:
			// In general, this pattern of access is not thread-safe. But since requests with the same itemBlockIdx
			// are sent to the same thread, it should be safe in our case.
			float[][] itemBlock = itemBlockMap.get(bp.itemBlockIdx);
			if (itemBlock == null) {
				itemBlock = MatrixUtils.generateRandomMatrix(config.getItemBlockLength(bp.itemBlockIdx), config.numLatent);
				itemBlockMap.put(bp.itemBlockIdx, itemBlock);
			}
			collector.emitDirect(taskId, new Values(ITEM_BLOCK, bp, (Object)itemBlock));
			break;
		case USER_BLOCK:
			userBlockMap.put(bp.userBlockIdx, (float[][])tuple.getValue(2));
			collector.emitDirect(tuple.getSourceTask(), new Values(USER_BLOCK_SAVED, bp, null));
			break;
		case ITEM_BLOCK:
			itemBlockMap.put(bp.itemBlockIdx, (float[][])tuple.getValue(2));
			collector.emitDirect(taskId, new Values(ITEM_BLOCK_SAVED, bp, null));
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(true, new Fields("msgType", "blockPair", "block"));
	}
}