package comparison.dsgd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Pair consisting of an IntWritable (the reducer number) and a MatrixItem
 * Used for 2 way sorting
 * @author christopherjohnson
 *
 */
public class IntMatrixItemPair implements WritableComparable<IntMatrixItemPair>{

	private IntWritable reducerNum;
	private MatrixItem matItem;
	
	public IntMatrixItemPair(){
		
	}
	
	public void set(IntWritable reducerNum, MatrixItem matItem){
		this.reducerNum = reducerNum;
		this.matItem = matItem;
	}
	
	public void readFields(DataInput arg0) throws IOException {
		reducerNum.readFields(arg0);
		matItem.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		reducerNum.write(arg0);
		matItem.write(arg0);
	}

	public int compareTo(IntMatrixItemPair o) {
		int cmp = reducerNum.compareTo(o.reducerNum);
		if (cmp != 0) {
			return cmp;
		}
		return matItem.compareTo(o.matItem);
	}

	public IntWritable getReducerNum() {
		return reducerNum;
	}

	public MatrixItem getMatItem() {
		return matItem;
	}

	public void setReducerNum(IntWritable reducerNum) {
		this.reducerNum = reducerNum;
	}

	public void setMatItem(MatrixItem matItem) {
		this.matItem = matItem;
	}

}
