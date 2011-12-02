package collabstream.streaming;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class StreamingDSGD {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("######## Wrong number of arguments");
			System.err.println("######## required args: local|production fileName");
			return;
		}
		
		Configuration config = new Configuration(17, 28, 3, 3, 6, 0.2f, 0.3f, 0.05f, 30, "", "", true);
		
		Config stormConfig = new Config();
		stormConfig.addSerialization(TrainingExample.Serialization.class);
		stormConfig.addSerialization(BlockPair.Serialization.class);
		stormConfig.addSerialization(MatrixSerialization.class);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(1, new RatingsSource());
		builder.setBolt(2, new Master(config))
			.globalGrouping(1)
			.globalGrouping(3, Worker.TO_MASTER_STREAM_ID);
		builder.setBolt(3, new Worker(config)).shuffleGrouping(2)
			.fieldsGrouping(2, Master.TO_WORKER_STREAM_ID, new Fields("userBlockIdx"))
			.directGrouping(4)
			.directGrouping(5);
		builder.setBolt(4, new MatrixStore(config))
			.allGrouping(2, Master.TO_MATRIX_STORE_STREAM_ID)
			.fieldsGrouping(3, Worker.USER_BLOCK_STREAM_ID, new Fields("userBlockIdx"));
		builder.setBolt(5, new MatrixStore(config))
			.allGrouping(2, Master.TO_MATRIX_STORE_STREAM_ID)
			.fieldsGrouping(3, Worker.ITEM_BLOCK_STREAM_ID, new Fields("itemBlockIdx"));
		
		System.out.println("######## StreamingDSGD.main: submitting topology");
		
		if ("local".equals(args[0])) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("StreamingDSGD", stormConfig, builder.createTopology());
		} else {
			StormSubmitter.submitTopology("StreamingDSGD", stormConfig, builder.createTopology());
		}
	}
}