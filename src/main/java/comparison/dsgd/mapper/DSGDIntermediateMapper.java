package comparison.dsgd.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import comparison.dsgd.DSGDMain;
import comparison.dsgd.IntMatrixItemPair;
import comparison.dsgd.MatrixItem;

public class DSGDIntermediateMapper extends
		Mapper<MatrixItem, NullWritable, IntMatrixItemPair, NullWritable> {

	int[] stratumArray;
	int numItems;
	int numUsers;
	int numReducers;
	IntMatrixItemPair intMatPair = new IntMatrixItemPair();
	IntWritable reducerNum = new IntWritable();
	MatrixItem matItem = new MatrixItem();
	NullWritable nw = NullWritable.get();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		String stratumString = context.getConfiguration().get("stratum");
		StringTokenizer st = new StringTokenizer(stratumString);
		numReducers = Integer.parseInt(context.getConfiguration().get(
				"numReducers"));
		stratumArray = new int[numReducers];
		for (int i = 0; i < numReducers; ++i) {
			stratumArray[i] = Integer.parseInt(st.nextToken());
		}
		numUsers = Integer.parseInt(context.getConfiguration().get("numUsers"));
		numItems = Integer.parseInt(context.getConfiguration().get("numItems"));
		super.setup(context);
	}

	public void map(MatrixItem key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		int row = key.getRow().get();
		int column = key.getColumn().get();
		if (key.isRatingsItem()) {
			int blockRow = DSGDMain.getBlockColumn(row, numUsers, numReducers);
			int blockColumn = DSGDMain.getBlockColumn(column, numItems,
					numReducers);
			// Emit if in current stratum
			if (stratumArray[blockColumn] == blockRow) {
				reducerNum.set(blockColumn);
				matItem = new MatrixItem();
				matItem.set(key.getRow().get(), key.getColumn().get(), key
						.getValue().get(), key.getMatrixType().toString());
				intMatPair.set(reducerNum, matItem);
				context.write(intMatPair, nw);
			}
		} else if (key.isFactorItem()) {
			if (key.getMatrixType().equals(MatrixItem.U_MATRIX)) {
//				System.out.println("UMatrix["
//				+ key.getRow()
//				+ "]["
//				+ key.getColumn().get()
//				+ "]: "
//				+ key.getValue());
				int blockRow = DSGDMain.getBlockColumn(row, numUsers,
						numReducers);
				reducerNum.set(stratumArray[blockRow]);
				matItem = new MatrixItem();
				matItem.set(key.getRow().get(), key.getColumn().get(), key
						.getValue().get(), key.getMatrixType().toString());
				intMatPair.set(reducerNum, matItem);
				context.write(intMatPair, nw);
			} else {
//				System.out.println("MMatrix["
//						+ key.getRow()
//						+ "]["
//						+ key.getColumn().get()
//						+ "]: "
//						+ key.getValue());
				int blockColumn = DSGDMain.getBlockColumn(row, numItems,
						numReducers);
				reducerNum.set(blockColumn);
				matItem = new MatrixItem();
				matItem.set(key.getRow().get(), key.getColumn().get(), key
						.getValue().get(), key.getMatrixType().toString());
				intMatPair.set(reducerNum, matItem);
				context.write(intMatPair, nw);
			}
		}
	}
}
