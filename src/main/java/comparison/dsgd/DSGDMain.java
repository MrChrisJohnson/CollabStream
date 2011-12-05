package comparison.dsgd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import comparison.dsgd.mapper.DSGDOutputFactorsMapper;
import comparison.dsgd.mapper.DSGDIntermediateMapper;
import comparison.dsgd.mapper.DSGDPreprocFactorMapper;
import comparison.dsgd.mapper.DSGDPreprocRatingsMapper;
import comparison.dsgd.mapper.DSGDRmseMapper;
import comparison.dsgd.reducer.DSGDOutputFactorsReducer;
import comparison.dsgd.reducer.DSGDIntermediateReducer;
import comparison.dsgd.reducer.DSGDPreprocFactorReducer;
import comparison.dsgd.reducer.DSGDPreprocRatingsReducer;
import comparison.dsgd.reducer.DSGDRmseReducer;


public class DSGDMain extends Configured {
	
	private static final Logger sLogger = Logger.getLogger(DSGDMain.class);
	
	public static int getBlockRow(long row, long numUsers, int numReducers) {
		long usersPerBlock = numUsers / (long) numReducers;
		return (int)(row / usersPerBlock);
	}

	public static int getBlockColumn(long column, long numItems, int numReducers) {
		long itemsPerBlock = numItems / (long) numReducers;
		return (int)(column / itemsPerBlock);
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("usage: [input-path] [output-path]");
			return;
		}
		long startTime = System.currentTimeMillis();
		long timeElapsed = 0;
		
		String input = args[0];
		String output = args[1];
		int numReducers = Integer.parseInt(args[2]);
		
		// Fixed parameters
		// Edit these for individual experiment
		String numUsers = "17771";
		String numItems = "2649430";
		int kValue = 1000;
		int numIterations = 2;
		double stepSize = 0.1; // \tau (step size for SGD updates)


		String trainInput = input + "/train";
		String testInput = input + "/test";
		String tempInput = output + "/temp/input";
		String tempOutput = output + "/temp/output/";
		String tempTrainRatings = output + "/temp/train";
		String tempTestRatings = output + "/temp/test";
		String emptyInput = input + "/empty";
		String finalFactorsOutput = output + "/final_factors";
		String rmseOutput = output + "/rmse";
		Path tempInputPath = new Path(tempInput);
		Path tempOutputPath = new Path(tempOutput);

		/**
		 * jobPreprocRatings
		 * write train and test ratings to RatingsItem format
		 */
		Configuration confPreprocRatings = new Configuration();
		confPreprocRatings.set("numUsers", numUsers);
		confPreprocRatings.set("numReducers", Integer.toString(numReducers)); 
		confPreprocRatings.set("numItems", numItems);
		confPreprocRatings.set("kValue", Integer.toString(kValue));

		Job jobPreprocRatings = new Job(confPreprocRatings, "train_ratings");
		
		FileInputFormat.setInputPaths(jobPreprocRatings, new Path(trainInput));
		FileOutputFormat.setOutputPath(jobPreprocRatings, new Path(tempTrainRatings));
		// Delete the output directory if it exists already
		FileSystem.get(confPreprocRatings).delete(new Path(tempTrainRatings), true);

		jobPreprocRatings.setOutputFormatClass(SequenceFileOutputFormat.class);

		jobPreprocRatings.setJarByClass(DSGDMain.class);
		jobPreprocRatings.setNumReduceTasks(numReducers);
		jobPreprocRatings.setOutputKeyClass(MatrixItem.class);
		jobPreprocRatings.setOutputValueClass(NullWritable.class);
		jobPreprocRatings.setMapOutputKeyClass(MatrixItem.class);
		jobPreprocRatings.setMapOutputValueClass(NullWritable.class);
		jobPreprocRatings.setMapperClass(DSGDPreprocRatingsMapper.class);
		jobPreprocRatings.setReducerClass(DSGDPreprocRatingsReducer.class);
		jobPreprocRatings.setCombinerClass(DSGDPreprocRatingsReducer.class);
		
		// train RatingsItems
		jobPreprocRatings.waitForCompletion(true);
		
		// test RatingsItems 
		jobPreprocRatings = new Job(confPreprocRatings, "train_ratings");
		
		FileInputFormat.setInputPaths(jobPreprocRatings, new Path(testInput));
		FileOutputFormat.setOutputPath(jobPreprocRatings, new Path(tempTestRatings));
		jobPreprocRatings.setOutputFormatClass(SequenceFileOutputFormat.class);
		jobPreprocRatings.setJarByClass(DSGDMain.class);
		jobPreprocRatings.setNumReduceTasks(numReducers);
		jobPreprocRatings.setOutputKeyClass(MatrixItem.class);
		jobPreprocRatings.setOutputValueClass(NullWritable.class);
		jobPreprocRatings.setMapOutputKeyClass(MatrixItem.class);
		jobPreprocRatings.setMapOutputValueClass(NullWritable.class);
		jobPreprocRatings.setMapperClass(DSGDPreprocRatingsMapper.class);
		jobPreprocRatings.setReducerClass(DSGDPreprocRatingsReducer.class);
		jobPreprocRatings.setCombinerClass(DSGDPreprocRatingsReducer.class);
		jobPreprocRatings.waitForCompletion(true);
		
		/**
		 * jobPreprocFactors
		 * Create initial U and M and output to FactorItem format
		 */
		
		Configuration confPreprocFactors = new Configuration();
		confPreprocRatings.set("numUsers", numUsers);
		confPreprocRatings.set("numItems", numItems);
		confPreprocRatings.set("kValue", Integer.toString(kValue));
		
		Job jobPreprocFactors = new Job(confPreprocFactors, "factors");
		
		FileInputFormat.setInputPaths(jobPreprocFactors, new Path(emptyInput));
		FileOutputFormat.setOutputPath(jobPreprocRatings, new Path(tempTrainRatings));
		// Delete the output directory if it exists already
		FileSystem.get(confPreprocRatings).delete(new Path(tempTrainRatings), true);
		
		jobPreprocFactors.setJarByClass(DSGDMain.class);
		jobPreprocFactors.setNumReduceTasks(1);
		jobPreprocFactors.setOutputKeyClass(MatrixItem.class);
		jobPreprocFactors.setOutputValueClass(NullWritable.class);
		jobPreprocFactors.setMapOutputKeyClass(MatrixItem.class);
		jobPreprocFactors.setMapOutputValueClass(NullWritable.class);
		jobPreprocFactors.setMapperClass(DSGDPreprocFactorMapper.class);
		jobPreprocFactors.setReducerClass(DSGDPreprocFactorReducer.class);
		jobPreprocFactors.setCombinerClass(DSGDPreprocFactorReducer.class);
		jobPreprocFactors.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		jobPreprocFactors.waitForCompletion(true);
		
		// Build stratum
		List<Integer> stratum = new ArrayList<Integer>();
		for(int i = 0; i < numReducers; ++i ){
			stratum.add(i);
		}
		String stratumString;
		for(int i=0; i < numIterations; ++i) {
			
			// Choose a random stratum
			Collections.shuffle(stratum);
			// Build string version of stratum
			stratumString = "";
			for(int j : stratum){
				stratumString += " " + j;
			}
			
			/**
			 * jobIntermediate
			 * Perform DSGD updates on stratum
			 */
			Configuration confIntermediate = new Configuration();
			confIntermediate.set("stratum", stratumString);
			confIntermediate.set("numUsers", numUsers);
			confIntermediate.set("numReducers", args[2]);
			confIntermediate.set("numItems", numItems);
			confIntermediate.set("kValue", Integer.toString(kValue));
			confIntermediate.set("stepSize", Double.toString(stepSize));
			FileSystem fs = FileSystem.get(confIntermediate);
			
			Job jobIntermediate = new Job(confIntermediate, "DSGD: Intermediate");

			FileInputFormat.setInputPaths(jobIntermediate, tempInputPath);
			FileOutputFormat.setOutputPath(jobIntermediate, tempOutputPath);
			jobIntermediate.setInputFormatClass(SequenceFileInputFormat.class);
			jobIntermediate.setOutputFormatClass(SequenceFileOutputFormat.class);

			jobIntermediate.setJarByClass(DSGDMain.class);
			jobIntermediate.setNumReduceTasks(numReducers);
			jobIntermediate.setOutputKeyClass(MatrixItem.class);
			jobIntermediate.setOutputValueClass(NullWritable.class);
			jobIntermediate.setMapOutputKeyClass(MatrixItem.class);
			jobIntermediate.setMapOutputValueClass(NullWritable.class);
			
			jobIntermediate.setMapperClass(DSGDIntermediateMapper.class);
			// job.setCombinerClass(IntermediateReducer.class);
			jobIntermediate.setReducerClass(DSGDIntermediateReducer.class);

			jobIntermediate.waitForCompletion(true);
			
			fs.delete(tempInputPath, true);
			fs.rename(tempOutputPath, tempInputPath);
			
			/**
			 * jobRmse
			 * Calculate RMSE on test data using current factors
			 */
			timeElapsed += (System.currentTimeMillis() - startTime);
			
			confIntermediate.set("timeElapsed", Long.toString(timeElapsed));
			Job jobRmse = new Job(confIntermediate, "DSGD: RMSE");
			FileInputFormat.setInputPaths(jobRmse, tempInput + "," + tempTestRatings);
			FileOutputFormat.setOutputPath(jobRmse, new Path(rmseOutput));
			jobRmse.setInputFormatClass(SequenceFileInputFormat.class);
			
			jobRmse.setJarByClass(DSGDMain.class);
			jobRmse.setNumReduceTasks(1);
			jobRmse.setOutputKeyClass(DoubleWritable.class);
			jobRmse.setOutputValueClass(DoubleWritable.class);
			jobRmse.setMapOutputKeyClass(MatrixItem.class);
			jobRmse.setMapOutputValueClass(NullWritable.class);
			jobRmse.setMapperClass(DSGDRmseMapper.class);
			jobRmse.setReducerClass(DSGDRmseReducer.class);  
			
			jobRmse.waitForCompletion(true);
			
			startTime = System.currentTimeMillis();
		}
		// Emit final factor matrices
//		Configuration confFinal = new Configuration();
//		confFinal.set("kValue", Integer.toString(kValue));
//		
//		Job jobFinal = new Job(confFinal, "ImprovedPBFS");
//		FileInputFormat.setInputPaths(jobFinal, tempInputPath);
//		FileOutputFormat.setOutputPath(jobFinal, new Path(finalOutput));
//
//		jobFinal.setInputFormatClass(SequenceFileInputFormat.class);
//		jobFinal.setJarByClass(DSGDMain.class);
//		jobFinal.setNumReduceTasks(1); // Must use 1 reducer so that both U, M, and R 
//									   // get sent to same reducer
//		
//		jobFinal.setOutputKeyClass(NullWritable.class);
//		jobFinal.setOutputValueClass(NullWritable.class);
//		
//		jobFinal.setMapOutputKeyClass(Matrix.class);
//		jobFinal.setMapOutputValueClass(NullWritable.class);
//		
//		jobFinal.setMapperClass(DSGDOutputFactorsMapper.class);
//		// jobFinal.setCombinerClass(FinalReducer.class);
//		jobFinal.setReducerClass(DSGDOutputFactorsReducer.class);
//
//		jobFinal.waitForCompletion(true);
//		// fsFinal.delete(tempInputDir, true);
	}

}
