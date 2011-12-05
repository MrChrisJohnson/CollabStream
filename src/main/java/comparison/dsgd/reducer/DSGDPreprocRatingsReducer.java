package comparison.dsgd.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import comparison.dsgd.MatrixItem;

public class DSGDPreprocRatingsReducer extends Reducer<MatrixItem, NullWritable, MatrixItem, NullWritable>{

	NullWritable nw = NullWritable.get();
	
	protected void reduce(MatrixItem key, Iterable<NullWritable> values, Context context) 
		throws IOException, InterruptedException {
		
		context.write(key, nw);
		
	}

}
