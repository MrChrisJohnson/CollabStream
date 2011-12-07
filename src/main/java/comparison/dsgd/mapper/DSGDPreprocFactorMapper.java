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
		context.write(new MatrixItem(), nw);
		super.setup(context);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	}
}


