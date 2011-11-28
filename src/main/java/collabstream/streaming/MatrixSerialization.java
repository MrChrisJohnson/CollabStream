package collabstream.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

import backtype.storm.serialization.ISerialization;

public class MatrixSerialization implements ISerialization<float[][]>{
	public boolean accept(Class c) {
		return float[][].class.equals(c);
	}
	
	public void serialize(float[][] matrix, DataOutputStream out) throws IOException {
		if (matrix == null) return;
		int numRows = matrix.length;
		int numCols = (numRows == 0) ? 0 : matrix[0].length;
		
		out.writeInt(numRows);
		out.writeInt(numCols);
		
		byte[] arr = new byte[4*numCols];
		ByteBuffer bbuf = ByteBuffer.wrap(arr);
		FloatBuffer fbuf = bbuf.asFloatBuffer();
		
		for (int i = 0; i < numRows; ++i) {
			if (matrix[i].length != numCols) {
				throw new Error("Rows of matrix have different sizes");
			}
			fbuf.put(matrix[i]);
			out.write(arr);
			fbuf.clear();
		}
	}
	
	public float[][] deserialize(DataInputStream in) throws IOException {
		int numRows = in.readInt();
		int numCols = in.readInt();
		float[][] matrix = new float[numRows][numCols];
		
		byte[] arr = new byte[4*numCols];
		ByteBuffer bbuf = ByteBuffer.wrap(arr);
		FloatBuffer fbuf = bbuf.asFloatBuffer();
		
		for (int i = 0; i < numRows; ++i) {
			in.readFully(arr);
			fbuf.get(matrix[i]);
			fbuf.clear();
		}

		return matrix;
	}
}