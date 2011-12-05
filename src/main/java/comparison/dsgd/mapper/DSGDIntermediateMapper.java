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
	long numItems;
	long numUsers;
	int numReducers;
	IntMatrixItemPair intMatPair = new IntMatrixItemPair();
	IntWritable reducerNum = new IntWritable();
	NullWritable nw = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		String stratumString = context.getConfiguration().get("stratum");
		int numReducers = Integer.parseInt(context.getConfiguration().get(
				"numReducers"));
		StringTokenizer st = new StringTokenizer(stratumString);
		stratumArray = new int[numReducers];
		for (int i = 0; i < numReducers; ++i) {
			stratumArray[i] = Integer.parseInt(st.nextToken());
		}

		numUsers = Long.parseLong(context.getConfiguration().get("numUsers"));
		numItems = Long.parseLong(context.getConfiguration().get("numItems"));
		numReducers = Integer.parseInt(context.getConfiguration().get(
				"numReducers"));
		super.setup(context);
	}

	public void map(MatrixItem key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		long row = key.getRow().get();
		long column = key.getColumn().get();
		if(key.isRatingsItem()){
			int blockRow = DSGDMain.getBlockColumn(row, numUsers, numReducers);
			int blockColumn = DSGDMain.getBlockColumn(column, numItems, numReducers);
			// Emit if in current stratum
			if(stratumArray[blockColumn] == blockRow){
				reducerNum.set(blockColumn);
				intMatPair.set(reducerNum, key);
				context.write(intMatPair, nw);
			}
		} else if(key.isFactorItem()){
			if(key.getMatrixType().equals(MatrixItem.U_MATRIX)){
				int blockRow = DSGDMain.getBlockColumn(row, numUsers, numReducers);
				reducerNum.set(stratumArray[blockRow]);
			} else{
				int blockColumn = DSGDMain.getBlockColumn(row, numItems, numReducers);
				reducerNum.set(blockColumn);
				intMatPair.set(reducerNum, key);
				context.write(intMatPair, nw);
			}
		}
	}
}
