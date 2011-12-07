package collabstream.streaming;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class WorkingBlock implements Serializable {
	public final Set<TrainingExample> examples = new HashSet<TrainingExample>();
	public float[][] userBlock = null;
	public float[][] itemBlock = null;
	public boolean waitingForBlocks = false;
	public boolean waitingForStorage = false;
	
	public String toString() {
		StringBuilder b = new StringBuilder(24*examples.size() + 72);
		
		b.append("examples={");
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
		
		b.append("\nuserBlock=\n").append(MatrixUtils.toString(userBlock));
		b.append("\nitemBlock=\n").append(MatrixUtils.toString(itemBlock));
		b.append("\nwaitingForBlocks=").append(waitingForBlocks);
		b.append("\nwaitingForStorage=").append(waitingForStorage);
		
		return b.toString();
	}
	
	public TrainingExample getLatestExample() {
		TrainingExample latest = null;
		for (TrainingExample ex : examples) {
			if (latest == null || latest.timestamp < ex.timestamp) {
				latest = ex;
			}
		}
		return latest;
	}
}