package comparison.dsgd.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import comparison.dsgd.IntMatrixItemPair;
import comparison.dsgd.MatrixItem;
import comparison.dsgd.MatrixUtils;

public class DSGDIntermediateReducer extends
		Reducer<IntMatrixItemPair, NullWritable, MatrixItem, NullWritable> {

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
				if (UMatrixSet[i][j]) {
					if (Double.isNaN(UMatrix[i][j])
							|| Double.isInfinite(UMatrix[i][j])) {
						System.out.println("cool");
					}
					matItem.set(i, j, UMatrix[i][j],
							MatrixItem.U_MATRIX.toString());
					context.write(matItem, nw);
				}
			}
		}

		// Emit MMatrix
		for (int i = 0; i < numItems; ++i) {
			for (int j = 0; j < kValue; ++j) {
				if (MMatrixSet[i][j]) {
					if (Double.isNaN(MMatrix[i][j])
							|| Double.isInfinite(MMatrix[i][j])) {
						System.out.println("cool");
					}
					matItem.set(i, j, MMatrix[i][j],
							MatrixItem.M_MATRIX.toString());
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

	protected void reduce(IntMatrixItemPair key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		matItem = key.getMatItem();

		if (matItem.isFactorItem()) {
			if (matItem.getMatrixType().equals(MatrixItem.U_MATRIX)) {
				UMatrix[matItem.getRow().get()][matItem.getColumn().get()] = matItem
						.getValue().get();
				UMatrixSet[matItem.getRow().get()][matItem.getColumn().get()] = true;
				// System.out.println("UMatrix["
				// + matItem.getRow()
				// + "]["
				// + matItem.getColumn().get()
				// + "]: "
				// + matItem.getValue().get());
			} else {
				MMatrix[matItem.getRow().get()][matItem.getColumn().get()] = matItem
						.getValue().get();
				MMatrixSet[matItem.getRow().get()][matItem.getColumn().get()] = true;
				// System.out.println("MMatrix["
				// + matItem.getRow()
				// + "]["
				// + matItem.getColumn().get()
				// + "]: "
				// + matItem.getValue().get());
			}
		} else { // If we are now processing Ratings Items then we must have the
					// full Factor Matrices
			// Perform update
			// forall l
			// U_il <-- U_il + 2*\tau*M_jl*(r-r') + (\lambda/m)U_il
			// M_jl <-- M_jl + 2*\tau*U_il*(r-r') + (\lambda/n)M_jl
			int i = matItem.getRow().get();
			int j = matItem.getColumn().get();
			double oldUval;
			double oldMval;
			double newUval;
			double newMval;
			// Get predicted value
			// build U_i vector
			double[] UVector = UMatrix[i];
			// System.out.println("UMatrix[" + i + "][" + 0 + "]: " +
			// UVector[0]);

			// build M_j vector
			double[] MVector = MMatrix[j];
			// System.out.println("MMatrix[" + j + "][" + 0 + "]: " +
			// MVector[0]);

			// Compute ratings prediction using dot product of factor vectors
			double prediction = MatrixUtils.dotProduct(UVector, MVector);

			// Update Factor Matrices
			for (int l = 0; l < kValue; ++l) {
				oldUval = UMatrix[i][l];
				oldMval = MMatrix[j][l];
				newUval = oldUval - (2*tau* (oldMval
						* (prediction - matItem.getValue().get()) + lambda
						* oldUval));
				newMval = oldMval - (2 * tau * (oldUval
						* (prediction - matItem.getValue().get()) + lambda
						* oldMval));
				UMatrix[i][l] = newUval;
				MMatrix[j][l] = newMval;
//				if (Double.isNaN(newUval) || Double.isInfinite(newUval)
//						|| newUval == Double.NEGATIVE_INFINITY) {
//					System.out.println("cool");
//				}
//				if (Double.isNaN(newMval) || Double.isInfinite(newMval)
//						|| newMval == Double.NEGATIVE_INFINITY) {
//					System.out.println("cool");
//				}
				if(Math.abs(newUval) > 100 || Math.abs(newMval) > 100){
					System.out.println("cool");
				}
			}
		}
	}

}
