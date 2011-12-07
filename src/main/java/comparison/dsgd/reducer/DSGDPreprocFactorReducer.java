package comparison.dsgd.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import comparison.dsgd.MatrixItem;

public class DSGDPreprocFactorReducer extends Reducer<MatrixItem, NullWritable, MatrixItem, NullWritable>{

	NullWritable nw = NullWritable.get();
	MatrixItem matItem = new MatrixItem();
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// Emit Factor Items (on the reduce side be sure to ignore duplicates
		// that may occur due to multiple mappers)
		int numUsers = Integer.parseInt(context.getConfiguration().get(
				"numUsers"));
		int numItems = Integer.parseInt(context.getConfiguration().get(
				"numItems"));
		int kValue = Integer.parseInt(context.getConfiguration().get("kValue"));
		for (int i = 0; i < numUsers; ++i) {
			for (int j = 0; j < kValue; ++j) {
				matItem.set(i, j, Math.random(), MatrixItem.U_MATRIX.toString());
				context.write(matItem, nw);
			}
		}
		for (int i = 0; i < numItems; ++i) {
			for (int j = 0; j < kValue; ++j) {
				matItem.set(i, j, Math.random(), MatrixItem.M_MATRIX.toString());
				context.write(matItem, nw);
			}
		}
		super.setup(context);
	}

	protected void reduce(MatrixItem key, Iterable<NullWritable> values, Context context) 
		throws IOException, InterruptedException {
		
	}

}
