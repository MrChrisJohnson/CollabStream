package comparison.dsgd.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DSGDPreprocMapper extends
Mapper<LongWritable, Text, Text, Text> {

	private HashMap<String, Set<String>> gramMap;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		int numReducers = Integer.parseInt(context.getConfiguration().get(
				"numReducers"));
		long numUsers = Long.parseLong(context.getConfiguration().get(
				"numUsers"));
		long numItems = Long.parseLong(context.getConfiguration().get(
				"numItems"));
		long user = key.get();
		long movie = 0;
		String line = ((Text) value).toString();
		StringTokenizer itr = new StringTokenizer(line);
		
		
		// build gramMap
		while (itr.hasMoreTokens()) {
			
			user++;
		}
	}
}
