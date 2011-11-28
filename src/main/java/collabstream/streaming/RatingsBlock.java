package collabstream.streaming;

import java.util.HashSet;
import java.util.Set;

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
}