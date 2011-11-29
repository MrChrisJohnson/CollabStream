package collabstream.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import backtype.storm.serialization.ISerialization;

public class TrainingExample implements Serializable {
	public final int userId, itemId;
	public final float rating;
	
	public TrainingExample(int userId, int itemId, float rating) {
		this.userId = userId;
		this.itemId = itemId;
		this.rating = rating;
	}
	
	public String toString() {
		return "(" + userId + "," + itemId + "," + rating + ")";
	}
	
	public boolean equals(Object obj) {
		if (obj instanceof TrainingExample) {
			TrainingExample ex = (TrainingExample)obj;
			return this.userId == ex.userId && this.itemId == ex.itemId;
		} else {
			return false;
		}
	}
	
	public int hashCode() {
		int userHi = userId >>> 16;
		int userLo = userId & 0xFFFF;
		int itemHi = itemId >>> 16;
		int itemLo = itemId & 0xFFFF;
		
		return ((userLo ^ itemHi) << 16) | (userHi ^ itemLo);
	}
	
	public static class Serialization implements ISerialization<TrainingExample> {
		public boolean accept(Class c) {
			return TrainingExample.class.equals(c);
		}
		
		public void serialize(TrainingExample ex, DataOutputStream out) throws IOException {
			out.writeInt(ex.userId);
			out.writeInt(ex.itemId);
			out.writeFloat(ex.rating);
		}
		
		public TrainingExample deserialize(DataInputStream in) throws IOException {
			return new TrainingExample(in.readInt(), in.readInt(), in.readFloat());
		}
	}
}