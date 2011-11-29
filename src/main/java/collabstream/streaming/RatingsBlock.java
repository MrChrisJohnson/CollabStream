package collabstream.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import backtype.storm.serialization.ISerialization;

public class RatingsBlock {
	public Set<TrainingExample> examples;
	
	public RatingsBlock() {
		this.examples = new HashSet<TrainingExample>();
	}
	
	public RatingsBlock(int numExs) {
		this.examples = new HashSet<TrainingExample>(numExs);
	}
	
	public String toString() {
		if (examples == null) return "";
		StringBuilder b = new StringBuilder(12*examples.size());
		b.append('{');
		
		boolean first = true;
		for (TrainingExample ex : examples) {
			if (first) {
				first = false;
			} else {
				b.append(", ");
			}
			b.append(ex.toString());
		}
		
		b.append('}');
		return b.toString();
	}

	public static class Serialization implements ISerialization<RatingsBlock> {
		public boolean accept(Class c) {
			return RatingsBlock.class.equals(c);
		}
		
		public void serialize(RatingsBlock block, DataOutputStream out) throws IOException {
			if (block == null || block.examples == null) return;
			out.writeInt(block.examples.size());
			
			TrainingExample.Serialization trExSer = new TrainingExample.Serialization();
			for (TrainingExample ex : block.examples) {
				trExSer.serialize(ex, out);
			}
		}
		
		public RatingsBlock deserialize(DataInputStream in) throws IOException {
			int numExs = in.readInt();
			RatingsBlock block = new RatingsBlock(numExs);
			
			TrainingExample.Serialization trExSer = new TrainingExample.Serialization();
			for (int i = 0; i < numExs; ++i) {
				block.examples.add(trExSer.deserialize(in));
			}
			return block;
		}
	}
}