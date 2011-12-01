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
	private Configuration config;
	private Map<Integer, float[][]> userBlockMap = new HashMap<Integer, float[][]>();
	private Map<Integer, float[][]> itemBlockMap = new HashMap<Integer, float[][]>();
	
	public void prepare(Map stormConfig, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
	}
	
	public void execute(Tuple tuple) {
		MsgType msgType = (MsgType)tuple.getValue(0);
		switch (msgType) {
		case USER_BLOCK_REQ:
			long start = System.currentTimeMillis();
			float[][] block = MatrixUtils.generateRandomMatrix(200000, 20);
			long end = System.currentTimeMillis();
			int numRows = block.length;
			int numCols = block[0].length;
			System.out.printf("######## MatrixStore.execute: %f %d\n", block[numRows-1][numCols-1], end - start);
			collector.emitDirect(tuple.getSourceTask(), new Values(USER_BLOCK, (Object)block));
			break;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(true, new Fields("msgType", "block"));
	}
}