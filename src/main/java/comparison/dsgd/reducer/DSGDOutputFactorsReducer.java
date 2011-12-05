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

	/**
	 * Output factor matrices on cleanup
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		String output = "U_Matrix\n";
		for(double[] vector : UMatrix){
			for(double val : vector){
				output += val + " ";
			}
			output += "\n";
		}
		output += "\nM_Matrx\n";
		for(double[] vector : MMatrix){
			for(double val : vector){
				output += val + " ";
			}
			output +="\n";
		}
		context.write(new Text(output), nw);
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		kValue = Integer.parseInt(context.getConfiguration().get("kValue"));
		tau = Double.parseDouble(context.getConfiguration().get("stepSize"));
		lambda = Double.parseDouble(context.getConfiguration().get("lambda"));
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
