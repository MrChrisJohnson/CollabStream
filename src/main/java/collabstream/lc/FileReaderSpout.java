package collabstream.lc;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
	private SpoutOutputCollector collector;
	private String fileName;
	private LineNumberReader in;
	
	public FileReaderSpout(String fileName) {
		this.fileName = fileName;
	}

	public boolean isDistributed() {
		return false;
	}

	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			in = new LineNumberReader(new FileReader(fileName));
		} catch (FileNotFoundException e){
			System.err.print("######## FileReaderSpout.nextTuple: " + e);
		}
	}

	public void close() {
		if (in == null) return;
		try {
			in.close();
		} catch (IOException e) {
			System.err.print("######## FileReaderSpout.nextTuple: " + e);
		}
	}

	public void ack(Object msgId) {
		System.out.println("######## FileReaderSpout.ack: msgId=" + msgId);
	}

	public void fail(Object msgId) {
		System.out.println("######## FileReaderSpout.fail: msgId=" + msgId);
	}

	public void nextTuple() {
		if (in == null) return;
		String line = null;
		
		try {
			line = in.readLine();
		} catch (IOException e) {
			System.err.print("######## FileReaderSpout.nextTuple: " + e);
		}
		
		if (line != null) {
			int lineNum = in.getLineNumber();
			System.out.println("######## FileReaderSpout.nextTuple: " + lineNum + ". " + line);
			collector.emit(new Values(lineNum, line), lineNum);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lineNum", "line"));
	}
}