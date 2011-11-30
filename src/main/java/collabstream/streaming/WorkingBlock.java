package collabstream.streaming;

import java.util.ArrayList;
import java.util.List;

public class WorkingBlock {
	public final List<WorkingExample> examples = new ArrayList<WorkingExample>();
	public float[][] userBlock = null;
	public float[][] itemBlock = null;
	public boolean waiting = false;
	
	public String toString() {
		StringBuilder b = new StringBuilder(24*examples.size() + 48);
		
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
		b.append("\nwaiting=").append(waiting);
		
		return b.toString();
	}
}