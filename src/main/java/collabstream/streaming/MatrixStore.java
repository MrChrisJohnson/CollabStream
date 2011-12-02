package collabstream.streaming;

import java.util.HashMap;
import java.util.Map;

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
	private final Map<Integer, float[][]> userBlockMap = new HashMap<Integer, float[][]>();
	private final Map<Integer, float[][]> itemBlockMap = new HashMap<Integer, float[][]>();
	
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
		if (config.debug) {
			System.out.println("######## MatrixStore.execute: " + msgType + " " + bp);
		}
		
		switch (msgType) {
		case USER_BLOCK_REQ:
			float[][] userBlock = userBlockMap.get(bp.userBlockIdx);
			if (userBlock == null) {
				userBlock = generateUserMatrix(config.getUserBlockLength(bp.userBlockIdx), config.numLatent);
				userBlockMap.put(bp.userBlockIdx, userBlock);
			}
			collector.emitDirect(tuple.getSourceTask(), new Values(USER_BLOCK, bp, (Object)userBlock));
			break;
		case ITEM_BLOCK_REQ:
			float[][] itemBlock = itemBlockMap.get(bp.itemBlockIdx);
			if (itemBlock == null) {
				itemBlock = generateItemMatrix(config.getItemBlockLength(bp.itemBlockIdx), config.numLatent);
				itemBlockMap.put(bp.itemBlockIdx, itemBlock);
			}
			collector.emitDirect(tuple.getSourceTask(), new Values(ITEM_BLOCK, bp, (Object)itemBlock));
			break;
		case USER_BLOCK:
			System.out.println("######## MatrixStore.execute: userBlock=\n" + MatrixUtils.toString((float[][])tuple.getValue(2)));
			userBlockMap.put(bp.userBlockIdx, (float[][])tuple.getValue(2));
			collector.emitDirect(tuple.getSourceTask(), new Values(USER_BLOCK_SAVED, bp, null));
			break;
		case ITEM_BLOCK:
			System.out.println("######## MatrixStore.execute: itemBlock=\n" + MatrixUtils.toString((float[][])tuple.getValue(2)));
			itemBlockMap.put(bp.itemBlockIdx, (float[][])tuple.getValue(2));
			collector.emitDirect(tuple.getSourceTask(), new Values(ITEM_BLOCK_SAVED, bp, null));
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(true, new Fields("msgType", "blockPair", "block"));
	}
	
	// for testing only
	private static float[][] generateUserMatrix(int numRows, int numCols) {
		float[][] matrix = new float[numRows][numCols];
		
		for (int i = 0; i < numRows; ++i) {
			for (int j = 0; j < numCols; ++j) {
				matrix[i][j] = 2.0f;
			}
		}
		return matrix;
	}
	
	// for testing only
	private static float[][] generateItemMatrix(int numRows, int numCols) {
		float[][] matrix = new float[numRows][numCols];
		
		for (int i = 0; i < numRows; ++i) {
			for (int j = 0; j < numCols; ++j) {
				matrix[i][j] = 3.0f;
			}
		}
		return matrix;
	}
}