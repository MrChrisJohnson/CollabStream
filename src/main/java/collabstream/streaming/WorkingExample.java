package collabstream.streaming;

public class WorkingExample extends TrainingExample {
	public int numTrainingIters = 0;
	
	public WorkingExample(TrainingExample ex) {
		super(ex.userId, ex.itemId, ex.rating);
	}
	
	public String toString() {
		return "(" + userId + "," + itemId + "," + rating + "," + numTrainingIters +")";
	}
}