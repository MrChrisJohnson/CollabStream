package collabstream.lc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MsgTracker implements IRichBolt {
	private OutputCollector collector;
	private int fileReaderId, counterId;
	private HashMap<Integer, Record> lnToRec = new HashMap<Integer, Record>();
	
	private static class Record {
		boolean read = false;
		boolean counted = false;
		
		Record() {}
		
		public String toString() {
			return "read=" + (read ? 1 : 0) + ", counted=" + (counted ? 1 : 0); 
		}
	}
	
	public MsgTracker(int fileReaderId, int counterId) {
		this.fileReaderId = fileReaderId;
		this.counterId = counterId;
	}
	
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void cleanup() {
		ArrayList<Integer> keys = new ArrayList<Integer>(lnToRec.keySet());
		Collections.sort(keys);
		for (Integer lineNum : keys) {
			System.out.println("######## MsgTracker.cleanup: " + lineNum + ". " + lnToRec.get(lineNum));
		}
	}
	
	public void execute(Tuple tuple) {
		Integer lineNum = tuple.getIntegerByField("lineNum");
		Record rec = lnToRec.get(lineNum);
		if (rec == null) {
			rec = new Record();
			lnToRec.put(lineNum, rec);
		}
		
		int streamId = tuple.getSourceStreamId();
		if (streamId == fileReaderId) {
			rec.read = true;
		} else if (streamId == counterId) {
			rec.counted = true;
		}
		collector.ack(tuple);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}