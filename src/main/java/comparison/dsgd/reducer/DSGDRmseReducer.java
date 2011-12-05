package comparison.dsgd.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import comparison.dsgd.MatrixItem;
import comparison.dsgd.MatrixUtils;
import comparison.dsgd.MatrixUtils.MatrixException;

public class DSGDRmseReducer extends
		Reducer<MatrixItem, NullWritable, DoubleWritable, DoubleWritable> {

	// HashMap<LongPair, Double> UMap;
	// HashMap<LongPair, Double> MMap;
	double[][] UMatrix;
	double[][] MMatrix;
	MatrixItem matItem;
	int kValue;
	double tau;
	double lambda;
	double timeElapsed;
	int numUsers;
	int numItems;
	double sqError = 0;
	long numRatings = 0;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// Calculate RMSE from Sq Error and emit along with time elapsed
		double rmse = sqError / (double)numRatings;
		context.write(new DoubleWritable(timeElapsed), new DoubleWritable(rmse));
		
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// UMap = new HashMap<LongPair, Double>();
		// MMap = new HashMap<LongPair, Double>();
		kValue = Integer.parseInt(context.getConfiguration().get("kValue"));
		tau = Double.parseDouble(context.getConfiguration().get("stepSize"));
		lambda = Double.parseDouble(context.getConfiguration().get("lambda"));
		numUsers = Integer.parseInt(context.getConfiguration().get("numUsers"));
		numItems = Integer.parseInt(context.getConfiguration().get("numItems"));
		timeElapsed = Double.parseDouble(context.getConfiguration().get("timeElapsed"));

		UMatrix = new double[numUsers][kValue];
		MMatrix = new double[numItems][kValue];

		super.setup(context);
	}

	@Override
	protected void reduce(MatrixItem key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		if (key.isFactorItem()) {
			if (matItem.getMatrixType().equals(MatrixItem.U_MATRIX)) {
				UMatrix[matItem.getRow().get()][matItem.getColumn().get()] = matItem
						.getValue().get();
			} else {
				// MMap.put(new LongPair(facItem.getRow().get(), facItem
				// .getColumn().get()), facItem.getValue().get());
				MMatrix[matItem.getRow().get()][matItem.getColumn().get()] = matItem
						.getValue().get();
			}
		} else { // If we are now processing RatingsItems then we must have the
					// full Factor Matrices
			int i = key.getRow().get();
			int j = key.getColumn().get();

			double[] UVector = UMatrix[i];
			double[] MVector = MMatrix[j];

			double prediction = 0;
			try {
				prediction = MatrixUtils.dotProduct(UVector, MVector);
			} catch (MatrixException e) {
				e.printStackTrace();
			}

			sqError += ((prediction - key.getValue().get()) * (prediction - key
					.getValue().get()));
			numRatings++;
		}
	}

}
