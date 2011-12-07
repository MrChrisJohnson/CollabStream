package comparison.dsgd.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import comparison.dsgd.MatrixItem;

public class DSGDPreprocRatingsMapper extends
		Mapper<LongWritable, Text, MatrixItem, NullWritable> {

	// reuse writable objects for efficiency
	MatrixItem matItem = new MatrixItem();
	NullWritable nw = NullWritable.get();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = ((Text) value).toString();
		StringTokenizer itr = new StringTokenizer(line);
		double dUser = Double.parseDouble(itr.nextToken()); //use double due to data's floating point format
		int user = (int)dUser;
		double dItem = Double.parseDouble(itr.nextToken());
		int item = (int)dItem;
		double rating = Double.parseDouble(itr.nextToken());
		
		if(rating != 0){ // don't do anything with un-rated items
			matItem.set(user, item, rating, MatrixItem.R_MATRIX.toString());
			context.write(matItem, nw);
		}
	}
}
