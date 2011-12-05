package comparison.dsgd.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import comparison.dsgd.MatrixItem;
import comparison.dsgd.MatrixUtils;
import comparison.dsgd.MatrixUtils.MatrixException;

public class DSGDIntermediateReducer extends
		Reducer<MatrixItem, NullWritable, MatrixItem, NullWritable> {

//	HashMap<LongPair, Double> UMap;
//	HashMap<LongPair, Double> MMap;
	double[][] UMatrix;
	double[][] MMatrix;
	boolean[][] UMatrixSet;
	boolean[][] MMatrixSet;
	MatrixItem matItem;
	NullWritable nw = NullWritable.get();
	int kValue;
	double tau;
	double lambda;
	int numUsers;
	int numItems;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		// Emit UMatrix
		for (int i = 0; i < numUsers; ++i) {
			for (int j = 0; j < kValue; ++j) {
				if(UMatrixSet[i][j]){
					matItem.set(i, j, UMatrix[i][j], MatrixItem.U_MATRIX.toString());
					context.write(matItem, nw);
				}
			}
		}

		// Emit MMatrix
		for (int i = 0; i < numItems; ++i) {
			for (int j = 0; j < kValue; ++j) {
				if(MMatrixSet[i][j]){
					matItem.set(i, j, MMatrix[i][j], MatrixItem.M_MATRIX.toString());
					context.write(matItem, nw);
				}
			}
		}

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
		UMatrixSet = new boolean[numUsers][kValue];
		MMatrixSet = new boolean[numItems][kValue];

		super.setup(context);
	}

	@Override
	protected void reduce(MatrixItem key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		if (key.isFactorItem()) {
			if (key.getMatrixType().equals(MatrixItem.U_MATRIX)) {
				UMatrix[matItem.getRow().get()][matItem.getColumn().get()] = matItem
						.getValue().get();
				UMatrixSet[matItem.getRow().get()][matItem.getColumn().get()] = true;
			} else {
				MMatrix[matItem.getRow().get()][matItem.getColumn().get()] = matItem
						.getValue().get();
				MMatrixSet[matItem.getRow().get()][matItem.getColumn().get()] = true;
			}
		} else { // If we are now processing Ratings Items then we must have the
					// full Factor Matrices
			// Perform update
			// forall l
			// U_il <-- U_il + 2*\tau*M_jl*(r-r') + (\lambda/m)U_il
			// M_jl <-- M_jl + 2*\tau*U_il*(r-r') + (\lambda/n)M_jl
			int i = key.getRow().get();
			int j = key.getColumn().get();
			double currentFac;
			double tempFac;
			// Get predicted value
			// build U_i vector
			double[] UVector = UMatrix[i];

			// build M_j vector
			double[] MVector = MMatrix[j];

			// Compute ratings prediction using dot product of factor vectors
			double prediction = 0;
			try {
				prediction = MatrixUtils.dotProduct(UVector, MVector);
			} catch (MatrixException e) {
				e.printStackTrace();
			}
			
			// Update Factor Matrices
			for(int l = 0; l < kValue; ++l){
				currentFac = UMatrix[i][l];
				tempFac = MMatrix[j][l];
				currentFac = currentFac + 2 * tau * tempFac
				* (key.getValue().get() - prediction);
				UMatrix[i][l] = currentFac;
			}
			for(int l = 0; l < kValue; ++l){
				currentFac = MMatrix[j][l];
				tempFac = UMatrix[i][l];
				currentFac = currentFac + 2 * tau * tempFac
					* (key.getValue().get() - prediction);
				MMatrix[j][l] = currentFac;
			}
		}
	}

}
