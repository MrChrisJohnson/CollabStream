package collabstream.streaming;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import static collabstream.streaming.MsgType.*;

public class RatingsSource implements IRichSpout {
	protected SpoutOutputCollector collector;
	private final Configuration config;
	private BufferedReader input;
	private int sequenceNum = 0;
	private long inputStartTime;
	
	public RatingsSource(Configuration config) {
		this.config = config;
	}
	
	public boolean isDistributed() {
		return false;
	}
	
	public void open(Map stormConfig, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		inputStartTime = System.currentTimeMillis();
		System.out.printf("######## Input started: %1$tY-%1$tb-%1$td %1$tT %tZ\n", inputStartTime);
		try {
			input = new BufferedReader(new FileReader(config.inputFilename));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void close() {
	}
	
	public void ack(Object msgId) {
	}
	
	public void fail(Object msgId) {
		System.err.println("######## RatingsSource.fail: " + msgId);
	}
	
	public void nextTuple() {
		if (input == null) return;
		try {
			String line = input.readLine();
			if (line == null) {
				long inputEndTime = System.currentTimeMillis();
				System.out.printf("######## Input finished: %1$tY-%1$tb-%1$td %1$tT %tZ\n", inputEndTime);
				System.out.println("######## Elapsed input time: "
					+ DurationFormatUtils.formatPeriod(inputStartTime, inputEndTime, "H:m:s") + " (h:m:s)");
				
				input.close();
				input = null;
				collector.emit(new Values(END_OF_DATA, null), END_OF_DATA);
			} else {
				try {
					String[] token = StringUtils.split(line, ' ');
					int userId = Integer.parseInt(token[0]);
					int itemId = Integer.parseInt(token[1]);
					float rating = Float.parseFloat(token[2]);
					
					TrainingExample ex = new TrainingExample(sequenceNum++, userId, itemId, rating);
					collector.emit(new Values(TRAINING_EXAMPLE, ex), ex);
					
					if (config.inputDelay > 0) {
						Utils.sleep(config.inputDelay);
					}
				} catch (Exception e) {
					System.err.println("######## RatingsSource.nextTuple: Could not parse line: " + line + "\n" + e);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msgType", "example"));
	}
}