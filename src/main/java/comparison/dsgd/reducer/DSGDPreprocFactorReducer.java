package comparison.dsgd.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import comparison.dsgd.MatrixItem;

public class DSGDPreprocFactorReducer extends Reducer<MatrixItem, NullWritable, MatrixItem, NullWritable>{

	NullWritable nw = NullWritable.get();
	MatrixItem currentMatItem = new MatrixItem();
	
	protected void reduce(MatrixItem key, Iterable<NullWritable> values, Context context) 
		throws IOException, InterruptedException {
		if(key.getMatrixType().equals(currentMatItem.getMatrixType())){
			if(key.getRow().equals(currentMatItem.getRow())){
				if(key.getColumn().equals(currentMatItem.getColumn())){
					// Don't re-emit factor items (will be duplicates if > 1 mapper)
					return;
				}
			}
		}
		
		context.write(key, nw);
		
	}

}
