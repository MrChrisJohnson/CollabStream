package collabstream.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import backtype.storm.serialization.ISerialization;

public class BlockPair implements Serializable {
	public final int userBlockIdx, itemBlockIdx;
	
	public BlockPair(int userBlockIdx, int itemBlockIdx) {
		this.userBlockIdx = userBlockIdx;
		this.itemBlockIdx = itemBlockIdx;
	}
	
	public String toString() {
		return "(" + userBlockIdx + "," + itemBlockIdx + ")";
	}
	
	public boolean equals(Object obj) {
		if (obj instanceof BlockPair) {
			BlockPair bp = (BlockPair)obj;
			return this.userBlockIdx == bp.userBlockIdx && this.itemBlockIdx == bp.itemBlockIdx;
		} else {
			return false;
		}
	}
	
	public int hashCode() {
		int userHi = userBlockIdx >>> 16;
		int userLo = userBlockIdx & 0xFFFF;
		int itemHi = itemBlockIdx >>> 16;
		int itemLo = itemBlockIdx & 0xFFFF;
		
		return ((userLo ^ itemHi) << 16) | (userHi ^ itemLo);
	}
	
	public static class Serialization implements ISerialization<BlockPair> {
		public boolean accept(Class c) {
			return BlockPair.class.equals(c);
		}
		
		public void serialize(BlockPair bp, DataOutputStream out) throws IOException {
			out.writeInt(bp.userBlockIdx);
			out.writeInt(bp.itemBlockIdx);
		}
		
		public BlockPair deserialize(DataInputStream in) throws IOException {
			return new BlockPair(in.readInt(), in.readInt());
		}
	}
}