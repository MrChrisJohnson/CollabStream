package collabstream.streaming;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.time.DurationFormatUtils;

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
	private PrintWriter userOutput, itemOutput;
	private BlockPair[][] blockPair;
	private TrainingExample[][] latestExample;
	private Set<BlockPair> unfinished = new HashSet<BlockPair>();
	private Set<BlockPair> freeSet = new HashSet<BlockPair>();
	private Queue<BlockPair> userBlockQueue = new LinkedList<BlockPair>();
	private Queue<BlockPair> itemBlockQueue = new LinkedList<BlockPair>();
	private boolean endOfData = false;
	private long startTime, outputStartTime = 0;
	private final Random random = new Random(); 
	
	public Master(Configuration config) {
		this.config = config;		
		blockPair = new BlockPair[config.numUserBlocks][config.numItemBlocks];
		latestExample = new TrainingExample[config.numUserBlocks][config.numItemBlocks];
	}
	
	public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		startTime = System.currentTimeMillis();
		System.out.printf("######## Training started: %1$tY-%1$tb-%1$td %1$tT %tZ\n", startTime);
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		MsgType msgType = (MsgType)tuple.getValue(0);
		if (config.debug && msgType != END_OF_DATA && msgType != PROCESS_BLOCK_FIN) {
			System.out.println("######## Master.execute: " + msgType + " " + tuple.getValue(1));
		}
		TrainingExample ex, latest;
		BlockPair bp, head;
		
		switch (msgType) {
		case END_OF_DATA:
			if (config.debug) {
				System.out.println("######## Master.execute: " + msgType);
			}
			endOfData = true;
			collector.ack(tuple);
			distributeWork();
			break;
		case TRAINING_EXAMPLE:
			ex = (TrainingExample)tuple.getValue(1);
			int userBlockIdx = config.getUserBlockIdx(ex.userId);
			int itemBlockIdx = config.getItemBlockIdx(ex.itemId);
			
			latest = latestExample[userBlockIdx][itemBlockIdx];
			if (latest == null || latest.timestamp < ex.timestamp) {
				latestExample[userBlockIdx][itemBlockIdx] = ex;
			}
			
			bp = blockPair[userBlockIdx][itemBlockIdx];
			if (bp == null) {
				bp = blockPair[userBlockIdx][itemBlockIdx] = new BlockPair(userBlockIdx, itemBlockIdx);
				unfinished.add(bp);
				freeSet.add(bp);
			}
			
			collector.emit(tuple, new Values(TRAINING_EXAMPLE, null, ex, userBlockIdx));
			collector.ack(tuple);
			distributeWork();
			break;
		case PROCESS_BLOCK_FIN:
			bp = (BlockPair)tuple.getValue(1);
			ex = (TrainingExample)tuple.getValue(2);
			if (config.debug) {
				System.out.println("######## Master.execute: " + msgType + " " + bp + " " + ex);
			}
			
			latest = latestExample[bp.userBlockIdx][bp.itemBlockIdx];
			if (latest.timestamp == ex.timestamp) {
				latest.numTrainingIters = ex.numTrainingIters;
				if (endOfData && latest.numTrainingIters >= config.maxTrainingIters) {
					unfinished.remove(bp);
				}
			}
			
			// numTrainingIters must be updated before free() is called to prevent finished blocks
			// from being added to the freeSet
			free(bp.userBlockIdx, bp.itemBlockIdx);
			
			if (unfinished.isEmpty()) {
				startOutput();
			} else {
				distributeWork();
			}
			break;
		case USER_BLOCK:
			bp = (BlockPair)tuple.getValue(1);
			float[][] userBlock = (float[][])tuple.getValue(2);
			head = userBlockQueue.remove();
			if (!head.equals(bp)) {
				throw new RuntimeException("Expected " + head + ", but received " + bp + " for " + USER_BLOCK);
			}
			writeUserBlock(bp.userBlockIdx, userBlock);
			requestNextUserBlock();
			break;
		case ITEM_BLOCK:
			bp = (BlockPair)tuple.getValue(1);
			float[][] itemBlock = (float[][])tuple.getValue(2);
			head = itemBlockQueue.remove();
			if (!head.equals(bp)) {
				throw new RuntimeException("Expected " + head + ", but received " + bp + " for " + ITEM_BLOCK);
			}
			writeItemBlock(bp.itemBlockIdx, itemBlock);
			requestNextItemBlock();
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Field "userBlockIdx" used solely for grouping
		declarer.declare(new Fields("msgType", "blockPair", "example", "userBlockIdx"));
	}
	
	private void lock(int userBlockIdx, int itemBlockIdx) {
		for (int j = 0; j < config.numItemBlocks; ++j) {
			BlockPair bp = blockPair[userBlockIdx][j];
			if (bp != null) {
				freeSet.remove(bp);
			}
		}
		for (int i = 0; i < config.numUserBlocks; ++i) {
			BlockPair bp = blockPair[i][itemBlockIdx];
			if (bp != null) {
				freeSet.remove(bp);
			}
		}
	}
	
	private void free(int userBlockIdx, int itemBlockIdx) {
		for (int j = 0; j < config.numItemBlocks; ++j) {
			BlockPair bp = blockPair[userBlockIdx][j];
			if (bp != null) {
				if (latestExample[userBlockIdx][j].numTrainingIters < config.maxTrainingIters) {
					freeSet.add(bp);
				}
			}
		}
		for (int i = 0; i < config.numUserBlocks; ++i) {
			BlockPair bp = blockPair[i][itemBlockIdx];
			if (bp != null) {
				if (latestExample[i][itemBlockIdx].numTrainingIters < config.maxTrainingIters) {
					freeSet.add(bp);
				}
			}
		}	
	}
	
	private void distributeWork() {
		ArrayList<BlockPair> freeList = new ArrayList<BlockPair>(freeSet.size());
		while (!freeSet.isEmpty()) {
			freeList.addAll(freeSet);
			int i = random.nextInt(freeList.size());
			BlockPair bp = freeList.get(i);
			lock(bp.userBlockIdx, bp.itemBlockIdx);
			collector.emit(new Values(PROCESS_BLOCK_REQ, bp, null, bp.userBlockIdx));
			freeList.clear();
		}
	}
	
	private void startOutput() {
		try {
			if (outputStartTime > 0) return;
			outputStartTime = System.currentTimeMillis();
			System.out.printf("######## Training finished: %1$tY-%1$tb-%1$td %1$tT %tZ\n", outputStartTime);
			System.out.println("######## Elapsed training time: "
				+ DurationFormatUtils.formatPeriod(startTime, outputStartTime, "H:m:s") + " (h:m:s)");
			System.out.printf("######## Output started: %1$tY-%1$tb-%1$td %1$tT %tZ\n", outputStartTime);
			
			userOutput = new PrintWriter(new BufferedWriter(new FileWriter(config.userOutputFilename)));
			itemOutput = new PrintWriter(new BufferedWriter(new FileWriter(config.itemOutputFilename)));
			
			for (int i = 0; i < config.numUserBlocks; ++i) {
				// Add the first block in row i to userBlockQueue
				for (int j = 0; j < config.numItemBlocks; ++j) {
					BlockPair bp = blockPair[i][j];
					if (bp != null) {
						userBlockQueue.add(bp);
						break;
					}
				}
			}
			for (int j = 0; j < config.numItemBlocks; ++j) {
				// Add the first block in column j to itemBlockQueue
				for (int i = 0; i < config.numUserBlocks; ++i) {
					BlockPair bp = blockPair[i][j];
					if (bp != null) {
						itemBlockQueue.add(bp);
						break;
					}
				}
			}
			
			requestNextUserBlock();
			requestNextItemBlock();
		} catch (IOException e) {
			System.err.println("######## Master.startOutput: " + e);
		}
	}
	
	private void writeUserBlock(int userBlockIdx, float[][] userBlock) {
		int userBlockStart = config.getUserBlockStart(userBlockIdx);
		for (int i = 0; i < userBlock.length; ++i) {
			userOutput.print(userBlockStart + i);
			for (int k = 0; k < config.numLatent; ++k) {
				userOutput.print(' ');
				userOutput.print(userBlock[i][k]);
			}
			userOutput.println();
		}
	}
	
	private void writeItemBlock(int itemBlockIdx, float[][] itemBlock) {
		int itemBlockStart = config.getItemBlockStart(itemBlockIdx);
		for (int j = 0; j < itemBlock.length; ++j) {
			itemOutput.print(itemBlockStart + j);
			for (int k = 0; k < config.numLatent; ++k) {
				itemOutput.print(' ');
				itemOutput.print(itemBlock[j][k]);
			}
			itemOutput.println();
		}		
	}
	
	private void endOutput() {
		long endTime = System.currentTimeMillis();
		System.out.printf("######## Output finished: %1$tY-%1$tb-%1$td %1$tT %tZ\n", endTime);
		System.out.println("######## Elapsed output time: "
			+ DurationFormatUtils.formatPeriod(outputStartTime, endTime, "H:m:s") + " (h:m:s)");
		System.out.println("######## Total elapsed time: "
			+ DurationFormatUtils.formatPeriod(startTime, endTime, "H:m:s") + " (h:m:s)");
	}
	
	private void requestNextUserBlock() {
		if (userBlockQueue.isEmpty()) {
			if (userOutput != null) {
				userOutput.close();
			}
			if (itemBlockQueue.isEmpty()) {
				endOutput();
			}
		} else {
			BlockPair bp = userBlockQueue.peek();
			collector.emit(new Values(USER_BLOCK_REQ, bp, null, bp.userBlockIdx));
		}
	}
	
	private void requestNextItemBlock() {
		if (itemBlockQueue.isEmpty()) {
			if (itemOutput != null) {
				itemOutput.close();
			}
			if (userBlockQueue.isEmpty()) {
				endOutput();
			}
		} else {
			BlockPair bp = itemBlockQueue.peek();
			collector.emit(new Values(ITEM_BLOCK_REQ, bp, null, bp.userBlockIdx));
		}
	}
}