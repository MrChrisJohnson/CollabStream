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

public class Worker implements IRichBolt {
	public static final int TO_MASTER_STREAM_ID = 1;
	public static final int USER_BLOCK_STREAM_ID = 2;
	public static final int ITEM_BLOCK_STREAM_ID = 3;
	
	private OutputCollector collector;
	private final Configuration config;
	private final Map<BlockPair, WorkingBlock> workingBlockMap = new ConcurrentHashMap<BlockPair, WorkingBlock>();
	
	public Worker(Configuration config) {
		this.config = config;
	}
	
	public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		MsgType msgType = (MsgType)tuple.getValue(0);
		if (config.debug && msgType != TRAINING_EXAMPLE) {
			System.out.println("######## Worker.execute: " + msgType + " " + tuple.getValue(1));
		}
		BlockPair bp;
		WorkingBlock workingBlock;
		
		switch (msgType) {
		case TRAINING_EXAMPLE:
			TrainingExample ex = (TrainingExample)tuple.getValue(2);
			if (config.debug) {
				System.out.println("######## Worker.execute: " + msgType + " " + ex);
			}
			bp = new BlockPair(config.getUserBlockIdx(ex.userId), config.getItemBlockIdx(ex.itemId));
			workingBlock = getWorkingBlock(bp);
			workingBlock.examples.add(ex);
			break;
		case PROCESS_BLOCK_REQ:
			bp = (BlockPair)tuple.getValue(1);
			workingBlock = getWorkingBlock(bp);			
			workingBlock.waitingForBlocks = true;
			collector.emit(USER_BLOCK_STREAM_ID, new Values(USER_BLOCK_REQ, bp, null, null, bp.userBlockIdx));
			collector.emit(ITEM_BLOCK_STREAM_ID, new Values(ITEM_BLOCK_REQ, bp, null, null, bp.itemBlockIdx));
			break;
		case USER_BLOCK_REQ:
			// Forward request from master
			bp = (BlockPair)tuple.getValue(1);
			collector.emit(USER_BLOCK_STREAM_ID, new Values(USER_BLOCK_REQ, bp, null, tuple.getSourceTask(), bp.userBlockIdx));
			break;
		case ITEM_BLOCK_REQ:
			// Forward request from master
			bp = (BlockPair)tuple.getValue(1);
			collector.emit(ITEM_BLOCK_STREAM_ID, new Values(ITEM_BLOCK_REQ, bp, null, tuple.getSourceTask(), bp.itemBlockIdx));
			break;
		case USER_BLOCK:
			bp = (BlockPair)tuple.getValue(1);
			float[][] userBlock = (float[][])tuple.getValue(2);
			workingBlock = getWorkingBlock(bp);
			
			if (workingBlock.waitingForBlocks) {
				workingBlock.userBlock = userBlock;
				if (workingBlock.itemBlock != null) {
					update(bp, workingBlock);
				}
			}
			break;
		case ITEM_BLOCK:
			bp = (BlockPair)tuple.getValue(1);
			float[][] itemBlock = (float[][])tuple.getValue(2);
			workingBlock = getWorkingBlock(bp);
			
			if (workingBlock.waitingForBlocks) {
				workingBlock.itemBlock = itemBlock;
				if (workingBlock.userBlock != null) {
					update(bp, workingBlock);
				}
			}
			break;
		case USER_BLOCK_SAVED:
			bp = (BlockPair)tuple.getValue(1);
			workingBlock = getWorkingBlock(bp);
			
			if (workingBlock.waitingForStorage) {
				workingBlock.userBlock = null;
				if (workingBlock.itemBlock == null) {
					workingBlock.waitingForStorage = false;
					collector.emit(TO_MASTER_STREAM_ID, new Values(PROCESS_BLOCK_FIN, bp, workingBlock.getLatestExample()));
				}
			}
			break;
		case ITEM_BLOCK_SAVED:
			bp = (BlockPair)tuple.getValue(1);
			workingBlock = getWorkingBlock(bp);
			
			if (workingBlock.waitingForStorage) {
				workingBlock.itemBlock = null;
				if (workingBlock.userBlock == null) {
					workingBlock.waitingForStorage = false;
					collector.emit(TO_MASTER_STREAM_ID, new Values(PROCESS_BLOCK_FIN, bp, workingBlock.getLatestExample()));
				}
			}
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Fields "userBlockIdx" and "itemBlockIdx" used solely for grouping
		declarer.declareStream(TO_MASTER_STREAM_ID, new Fields("msgType", "blockPair", "latestExample"));
		declarer.declareStream(USER_BLOCK_STREAM_ID, new Fields("msgType", "blockPair", "block", "taskId", "userBlockIdx"));
		declarer.declareStream(ITEM_BLOCK_STREAM_ID, new Fields("msgType", "blockPair", "block", "taskId", "itemBlockIdx"));
	}
	
	private WorkingBlock getWorkingBlock(BlockPair bp) {
		// In general, this pattern of access is not thread-safe. But since requests with the same userBlockIdx
		// are sent to the same thread, it should be safe in our case.
		WorkingBlock workingBlock = workingBlockMap.get(bp);
		if (workingBlock == null) {
			workingBlock = new WorkingBlock();
			workingBlockMap.put(bp, workingBlock);
		}
		return workingBlock;
	}
	
	private void update(BlockPair bp, WorkingBlock workingBlock) {
		int userBlockStart = config.getUserBlockStart(bp.userBlockIdx);
		int itemBlockStart = config.getItemBlockStart(bp.itemBlockIdx);
		float[][] userBlock = workingBlock.userBlock;
		float[][] itemBlock = workingBlock.itemBlock;
		
		PermutationUtils.permute(workingBlock.examples);
		
		for (TrainingExample ex : workingBlock.examples) {
			if (ex.numTrainingIters >= config.maxTrainingIters) continue;
			int i = ex.userId - userBlockStart;
			int j = ex.itemId - itemBlockStart;
			
			float dotProduct = 0.0f;
			for (int k = 0; k < config.numLatent; ++k) {
				dotProduct += userBlock[i][k] * itemBlock[j][k];
			}
			float ratingDiff = dotProduct - ex.rating;
			
			++ex.numTrainingIters;
			float stepSize = 2 * config.initialStepSize / ex.numTrainingIters;
			
			for (int k = 0; k < config.numLatent; ++k) {
				float oldUserWeight = userBlock[i][k];
				float oldItemWeight = itemBlock[j][k];
				userBlock[i][k] -= stepSize*(ratingDiff * oldItemWeight + config.userPenalty * oldUserWeight);
				itemBlock[j][k] -= stepSize*(ratingDiff * oldUserWeight + config.itemPenalty * oldItemWeight);
			}
		}
		workingBlock.waitingForBlocks = false;
		workingBlock.waitingForStorage = true;
		
		collector.emit(USER_BLOCK_STREAM_ID, new Values(USER_BLOCK, bp, userBlock, null, bp.userBlockIdx));
		collector.emit(ITEM_BLOCK_STREAM_ID, new Values(ITEM_BLOCK, bp, itemBlock, null, bp.itemBlockIdx));		
	}
}