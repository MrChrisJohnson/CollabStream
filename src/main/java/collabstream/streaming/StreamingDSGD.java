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
		
		Config stormConfig = new Config();
		stormConfig.addSerialization(TrainingExample.Serialization.class);
		stormConfig.addSerialization(BlockPair.Serialization.class);
		stormConfig.addSerialization(MatrixSerialization.class);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(1, new RatingsSource());
		builder.setBolt(2, new Master()).shuffleGrouping(1);
		builder.setBolt(3, new Worker()).shuffleGrouping(2).directGrouping(4);
		builder.setBolt(4, new MatrixStore()).shuffleGrouping(3);
		
		System.out.println("######## StreamingDSGD.main: submitting topology");
		
		if ("local".equals(args[0])) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("StreamingDSGD", stormConfig, builder.createTopology());
		} else {
			StormSubmitter.submitTopology("StreamingDSGD", stormConfig, builder.createTopology());
		}
	}
}