package collabstream.streaming;

public class Configuration {
	public final int numUsers, numItems, numLatent;
	public final int numUserBlocks, numItemBlocks;
	private final int smallUserBlockSize, smallItemBlockSize;
	private final int bigUserBlockSize, bigItemBlockSize;
	private final int numBigUserBlocks, numBigItemBlocks;
	private final int userBlockThreshold, itemBlockThreshold;
	
	public Configuration(int numUsers, int numItems, int numLatent, int numUserBlocks, int numItemBlocks) {
		this.numUsers = numUsers;
		this.numItems = numItems;
		this.numLatent = numLatent;
		this.numUserBlocks = numUserBlocks;
		this.numItemBlocks = numItemBlocks;
		
		smallUserBlockSize = numUsers / numUserBlocks;
		bigUserBlockSize = smallUserBlockSize + 1;
		numBigUserBlocks = numUsers % numUserBlocks;
		userBlockThreshold = bigUserBlockSize * numBigUserBlocks; 
		
		smallItemBlockSize = numItems / numItemBlocks;
		bigItemBlockSize = smallItemBlockSize + 1;
		numBigItemBlocks = numItems % numItemBlocks;
		itemBlockThreshold = bigItemBlockSize * numBigItemBlocks;
	}
	
	public int getUserBlockIdx(int userId) {
		if (userId < userBlockThreshold) {
			return userId / bigUserBlockSize; 
		} else {
			return numBigUserBlocks + (userId - userBlockThreshold) / smallUserBlockSize;
		}
	}
	
	public int getItemBlockIdx(int itemId) {
		if (itemId < itemBlockThreshold) {
			return itemId / bigItemBlockSize; 
		} else {
			return numBigItemBlocks + (itemId - itemBlockThreshold) / smallItemBlockSize;
		}
	}
	
	public int getUserBlockLength(int userBlockIdx) {
		return (userBlockIdx < numBigUserBlocks) ? bigUserBlockSize : smallUserBlockSize;
	}
	
	public int getItemBlockLength(int itemBlockIdx) {
		return (itemBlockIdx < numBigItemBlocks) ? bigItemBlockSize : smallItemBlockSize;
	}
}