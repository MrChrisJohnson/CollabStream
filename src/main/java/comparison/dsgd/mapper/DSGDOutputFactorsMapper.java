package comparison.dsgd.mapper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import comparison.dsgd.MatrixItem;

public class DSGDOutputFactorsMapper extends Mapper<Text, MatrixItem, MatrixItem, NullWritable>{
	
	public void map(MatrixItem key, NullWritable value, Context context)
		throws IOException, InterruptedException {
		
		context.write(key, value);
	}
}
