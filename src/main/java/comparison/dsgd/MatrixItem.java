package comparison.dsgd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MatrixItem implements WritableComparable<MatrixItem>, Cloneable {

	private IntWritable row = new IntWritable();
	private IntWritable column = new IntWritable();
	private DoubleWritable value = new DoubleWritable();
	private Text matrixType = new Text();

	public static final Text U_MATRIX = new Text("U_MATRIX");
	public static final Text M_MATRIX = new Text("M_MATRIX");
	public static final Text R_MATRIX = new Text("R_MATRIX");

	public boolean isRatingsItem(){
		return matrixType.equals(R_MATRIX);
	}
	
	public boolean isFactorItem(){
		return !matrixType.equals(R_MATRIX);
	}
	
	public void set(int r, int c, double v, String m) {
		setRow(new IntWritable(r));
		setColumn(new IntWritable(c));
		setValue(new DoubleWritable(v));
		setMatrixType(new Text(m));
	}

	public void readFields(DataInput arg0) throws IOException {
		row.readFields(arg0);
		column.readFields(arg0);
		value.readFields(arg0);
		matrixType.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		row.write(arg0);
		column.write(arg0);
		value.write(arg0);
		matrixType.write(arg0);
	}

	public MatrixItem clone() {
		MatrixItem matItem = new MatrixItem();
		matItem.set(this.row.get(), this.column.get(), this.value.get(),
				new String(this.matrixType.toString()));
		return matItem;
	}

	public int compareTo(MatrixItem o) {
		int compare = this.getMatrixType().compareTo(o.getMatrixType());
		if(compare != 0){
			if(this.isRatingsItem()){
				return 1;
			} else if(o.isRatingsItem()){
				return -1;
			}
			return compare;
		}
//		compare = this.getRow().compareTo(o.getRow());
//		if(compare != 0){
//			return compare;
//		}
//		compare = this.getColumn().compareTo(o.getColumn());
//		if(compare != 0){
//			return compare;
//		}
//		return this.getValue().compareTo(o.getValue());
		double rand = Math.random();
		if(rand > 0.5){
			return 1;
		} else{
			return -1;
		}
	}

	public IntWritable getRow() {
		return row;
	}

	public IntWritable getColumn() {
		return column;
	}

	public DoubleWritable getValue() {
		return value;
	}

	public void setRow(IntWritable row) {
		this.row = row;
	}

	public void setColumn(IntWritable column) {
		this.column = column;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}

	public Text getMatrixType() {
		return matrixType;
	}

	public void setMatrixType(Text matrixType) {
		this.matrixType = matrixType;
	}

	// public IntWritable getRow();
	// public IntWritable getColumn();
	// public MatrixItem clone();

}
