package collabstream.streaming;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class StreamingDSGD {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("######## Wrong number of arguments");
			System.err.println("######## required args: local|production fileName");
			return;
		}
		
		Config config = new Config();
		config.addSerialization(TrainingExample.Serialization.class);
		config.addSerialization(RatingsBlock.Serialization.class);
		config.addSerialization(MatrixSerialization.class);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		System.out.println("######## StreamingDSGD.main: submitting topology");
		
		if ("local".equals(args[0])) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("StreamingDSGD", config, builder.createTopology());
		} else {
			StormSubmitter.submitTopology("StreamingDSGD", config, builder.createTopology());
		}
	}
}