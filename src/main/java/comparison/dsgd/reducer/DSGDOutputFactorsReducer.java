package comparison.dsgd.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import comparison.dsgd.MatrixItem;

/**
 * Note: DGSDFinalReducer requires a single reducer!!
 * 
 * @author christopherjohnson
 * 
 */
public class DSGDOutputFactorsReducer extends
		Reducer<MatrixItem, NullWritable, Text, NullWritable> {

	double[][] UMatrix;
	double[][] MMatrix;
	MatrixItem matItem;
	NullWritable nw = NullWritable.get();
	int kValue;
	double tau;
	double lambda;
	int numUsers;
	int numItems;
	Text text = new Text();

	/**
	 * Output factor matrices on cleanup
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		text.set("U_Matrix");
		context.write(text, nw);
		for(double[] UVector : UMatrix){
			String current = "";
			for(double d : UVector){
				current += d + " ";
			}
			text.set(current);
			context.write(text, nw);
		}
		text.set("M_Matrix");
		context.write(text, nw);
		
		for(double[] MVector : MMatrix){
			String current = "";
			for(double d : MVector){
				current += d + " ";
			}
			text.set(current);
			context.write(text, nw);
		}
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		kValue = Integer.parseInt(context.getConfiguration().get("kValue"));
		numUsers = Integer.parseInt(context.getConfiguration().get("numUsers"));
		numItems = Integer.parseInt(context.getConfiguration().get("numItems"));

		UMatrix = new double[numUsers][kValue];
		MMatrix = new double[numItems][kValue];

		
		super.setup(context);
	}

	@Override
	protected void reduce(MatrixItem key, Iterable<NullWritable> values, Context context) 
		throws IOException, InterruptedException {
		if(key.getMatrixType().equals(MatrixItem.U_MATRIX)){
			UMatrix[key.getRow().get()][key.getColumn().get()] = key.getValue().get();
		} else{
			MMatrix[key.getRow().get()][key.getColumn().get()] = key.getValue().get();
		}
	}
}
