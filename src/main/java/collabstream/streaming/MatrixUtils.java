package collabstream.streaming;

import backtype.storm.tuple.Values;

public class MatrixUtils {
	public static String toString(float[][] matrix) {
		if (matrix == null) return "";
		int numRows = matrix.length;
		if (numRows == 0) return "[]";
		int numCols = matrix[0].length;
		
		StringBuilder b = new StringBuilder(numRows*(9*numCols + 2) + 1);
		
		b.append('[');
		for (int i = 0; i < numRows-1; ++i) {
			numCols = matrix[i].length;
			for (int j = 0; j < numCols; ++j) {
				b.append(String.format(" %8.3f", matrix[i][j]));
			}
			b.append("\n ");
		}
		numCols = matrix[numRows-1].length;
		for (int j = 0; j < numCols; ++j) {
			b.append(String.format(" %8.3f", matrix[numRows-1][j]));
		}
		b.append(" ]");
		
		return b.toString();
	}
}