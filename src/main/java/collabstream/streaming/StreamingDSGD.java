package collabstream.streaming;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class StreamingDSGD {
	public static void main(String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("######## Wrong number of arguments");
			System.err.println("######## required args: local|production numUsers numItems"
							   + " inputFilename userOutputFilename itemOutputFilename");
			return;
		}
		
		Properties props = new Properties();
		File propFile = new File("data/collabstream.properties");
		if (propFile.exists()) {
			FileReader in = new FileReader(propFile);
			props.load(in);
			in.close();
		}
		
		int numUsers = Integer.parseInt(args[1]);
		int numItems = Integer.parseInt(args[2]);
		int numLatent = Integer.parseInt(props.getProperty("numLatent", "10"));
		int numUserBlocks = Integer.parseInt(props.getProperty("numUserBlocks", "10"));
		int numItemBlocks = Integer.parseInt(props.getProperty("numItemBlocks", "10"));
		float userPenalty = Float.parseFloat(props.getProperty("userPenalty", "0.1"));
		float itemPenalty = Float.parseFloat(props.getProperty("itemPenalty", "0.1"));
		float initialStepSize = Float.parseFloat(props.getProperty("initialStepSize", "0.1"));
		int maxTrainingIters = Integer.parseInt(props.getProperty("maxTrainingIters", "30"));
		String inputFilename = args[3];
		String userOutputFilename = args[4];
		String itemOutputFilename = args[5];
		long inputDelay = Long.parseLong(props.getProperty("inputDelay", "0"));
		boolean debug = Boolean.parseBoolean(props.getProperty("debug", "false"));

		Configuration config = new Configuration(numUsers, numItems, numLatent, numUserBlocks, numItemBlocks,
												 userPenalty, itemPenalty, initialStepSize, maxTrainingIters,
												 inputFilename, userOutputFilename, itemOutputFilename,
												 inputDelay, debug);
		
		Config stormConfig = new Config();
		stormConfig.addSerialization(TrainingExample.Serialization.class);
		stormConfig.addSerialization(BlockPair.Serialization.class);
		stormConfig.addSerialization(MatrixSerialization.class);
		stormConfig.setNumWorkers(config.getNumProcesses());
		stormConfig.setNumAckers(config.getNumWorkers()); // our notion of a worker is different from Storm's
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(1, new RatingsSource(config));
		builder.setBolt(2, new Master(config))
			.globalGrouping(1)
			.globalGrouping(3, Worker.TO_MASTER_STREAM_ID)
			.directGrouping(4)
			.directGrouping(5);
		builder.setBolt(3, new Worker(config), config.getNumWorkers())
			.fieldsGrouping(2, new Fields("userBlockIdx"))
			.directGrouping(4)
			.directGrouping(5);
		builder.setBolt(4, new MatrixStore(config), config.numUserBlocks)
			.fieldsGrouping(3, Worker.USER_BLOCK_STREAM_ID, new Fields("userBlockIdx"));
		builder.setBolt(5, new MatrixStore(config), config.numItemBlocks)
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