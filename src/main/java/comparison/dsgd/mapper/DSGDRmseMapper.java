package comparison.dsgd.mapper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import comparison.dsgd.MatrixItem;

public class DSGDRmseMapper extends Mapper<MatrixItem, NullWritable, MatrixItem, NullWritable>{
	
	NullWritable nw = NullWritable.get();
	
	public void map(MatrixItem key, NullWritable value, Context context)
		throws IOException, InterruptedException {
		
		context.write(key, nw);
		
	}
}
