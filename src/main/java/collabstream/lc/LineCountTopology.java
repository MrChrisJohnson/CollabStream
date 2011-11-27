package collabstream.lc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Simple line-counting program to test the functionality of Storm. Instantiates four classes: FileReaderSpout,
 * CounterBolt, MsgForwarder, and MsgTracker. FileReaderSpout reads lines from a file and emits them. CounterBolt
 * counts the lines received from FileReaderSpout and emits their corresponding line numbers. MsgForwarder just forwards
 * the lines received from FileReaderSpout and the line numbers received from CounterBolt. MsgTracker receives the
 * messages forwarded by MsgForwarder and keeps track of which lines were read and which lines were counted.
 */
public class LineCountTopology {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("######## Wrong number of arguments");
			System.err.println("######## required args: local|production fileName");
			return;
		}
		
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(1, new FileReaderSpout(args[1]));
		builder.setBolt(2, new CounterBolt()).shuffleGrouping(1);
		builder.setBolt(3, new MsgForwarder(1,2)).shuffleGrouping(1).shuffleGrouping(2);
		builder.setBolt(4, new MsgTracker(1,2)).shuffleGrouping(3,1).shuffleGrouping(3,2);
		
		System.out.println("######## LineCountTopology.main: submitting topology");
		
		if ("local".equals(args[0])) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("line-count", config, builder.createTopology());
			System.out.println("######## LineCountTopology.main: sleeping for 10 secs");
			Utils.sleep(10000);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology("line-count", config, builder.createTopology());
		}
	}
}