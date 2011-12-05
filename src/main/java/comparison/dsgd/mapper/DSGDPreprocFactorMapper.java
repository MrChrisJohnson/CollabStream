package comparison.dsgd.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import comparison.dsgd.MatrixItem;

public class DSGDPreprocFactorMapper extends
	Mapper<LongWritable, Text, MatrixItem, NullWritable> {
	
	MatrixItem matItem = new MatrixItem(); // reuse writable objects for efficiency
	//RatingsItem ratItem = new RatingsItem();
	NullWritable nw = NullWritable.get();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// Emit Factor Items (on the reduce side be sure to ignore duplicates
		// that may occur due to multiple mappers)
		long numUsers = Long.parseLong(context.getConfiguration().get(
				"numUsers"));
		long numItems = Long.parseLong(context.getConfiguration().get(
				"numItems"));
		int kValue = Integer.parseInt(context.getConfiguration().get("kValue"));
		for (int i = 0; i < numUsers; ++i) {
			for (int j = 0; j < kValue; ++j) {
				matItem.set(i, j, (double)0, MatrixItem.U_MATRIX.toString());
				context.write(matItem, nw);
			}
		}
		for (int i = 0; i < numItems; ++i) {
			for (int j = 0; j < kValue; ++j) {
				matItem.set(i, j, (double)0, MatrixItem.M_MATRIX.toString());
				context.write(matItem, nw);
			}
		}
		super.setup(context);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		//Do Nothing since factors are emitted in setup
	}
}


