package collabstream.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import backtype.storm.serialization.ISerialization;

public class RatingsBlockSerialization implements ISerialization<RatingsBlock> {
	public boolean accept(Class c) {
		System.out.println("######## RatingsBlockSerialization.accept");
		return RatingsBlock.class.isAssignableFrom(c);
	}
	
	public void serialize(RatingsBlock block, DataOutputStream out) throws IOException {
		System.out.println("######## RatingsBlockSerialization.serialize");
		if (block == null || block.examples == null) return;
		out.writeInt(block.examples.size());
		
		TrainingExample.Serialization trExSer = new TrainingExample.Serialization();
		for (TrainingExample ex : block.examples) {
			trExSer.serialize(ex, out);
		}
	}
	
	public RatingsBlock deserialize(DataInputStream in) throws IOException {
		System.out.println("######## RatingsBlockSerialization.deserialize");
		int numExs = in.readInt();
		RatingsBlock block = new RatingsBlock(numExs);
		
		TrainingExample.Serialization trExSer = new TrainingExample.Serialization();
		for (int i = 0; i < numExs; ++i) {
			block.examples.add(trExSer.deserialize(in));
		}
		return block;
	}
}