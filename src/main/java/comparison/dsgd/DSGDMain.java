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
	
	public static int getBlockRow(int row, int numUsers, int numReducers) {
		int usersPerBlock = (numUsers / numReducers) + 1;
		return (int)Math.floor(((double)row / (double)usersPerBlock));
	}

	public static int getBlockColumn(int column, int numItems, int numReducers) {
		int itemsPerBlock = (numItems / numReducers) + 1;
		return (int)Math.floor(((double)column / (double)itemsPerBlock));
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
		String numUsers = "573";
		String numItems = "2649430";
		int kValue = 10;
		int numIterations = 10;
		int numBlocks = numReducers;
		double stepSize = 0.1; // \tau (step size for SGD updates)
		double lambda = 0.1;
		
		String trainInput = input + "/train";
		String testInput = input + "/test";
		String tempFactorsInput = output + "/temp/input";
		String tempFactorsOutput = output + "/temp/output/";
		String tempTrainRatings = output + "/temp/train";
		String tempTestRatings = output + "/temp/test";
		String emptyInput = input + "/empty";
		String finalFactorsOutput = output + "/final_factors";
		String rmseOutput = output + "/rmse";

		/**
		 * jobPreprocRatings
		 * write train and test ratings to RatingsItem format
		 */
		Configuration confPreprocRatings = new Configuration();
		confPreprocRatings.set("numUsers", numUsers);
		confPreprocRatings.set("numReducers", Integer.toString(numBlocks)); 
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
		// Delete the output directory if it exists already
		FileSystem.get(confPreprocRatings).delete(new Path(tempTestRatings), true);
		
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
		confPreprocFactors.set("numUsers", numUsers);
		confPreprocFactors.set("numItems", numItems);
		confPreprocFactors.set("kValue", Integer.toString(kValue));
		
		Job jobPreprocFactors = new Job(confPreprocFactors, "factors");
		
		FileInputFormat.setInputPaths(jobPreprocFactors, new Path(emptyInput));
		FileOutputFormat.setOutputPath(jobPreprocFactors, new Path(tempFactorsInput));
		// Delete the output directory if it exists already
		FileSystem.get(confPreprocRatings).delete(new Path(tempFactorsInput), true);
		
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
		
//		if(true){
//			return;
//		}
		
		for(int a = 0; numIterations < 1; ++a){
			sLogger.info("Running iteration " + a);
			// Build stratum
			List<Integer> stratum = new ArrayList<Integer>();
			for(int i = 0; i < numBlocks; ++i ){
				stratum.add(i);
			}
			Collections.shuffle(stratum); //choose random initial stratum
			String stratumString;
			
			for(int i=0; i < numReducers; ++i) {
				
				sLogger.info("Running stratum " + i + " on iteration " + a );
				// Choose next stratum
				int current1 = stratum.get(0);
				int current2;
				for(int j = 1; j < numBlocks; ++j){
					current2 = stratum.get(j);
					stratum.set(j, current1);
					current1 = current2;
				}
				stratum.set(0, current1);
				
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
				confIntermediate.set("numReducers", Integer.toString(numBlocks));
				confIntermediate.set("numItems", numItems);
				confIntermediate.set("kValue", Integer.toString(kValue));
				confIntermediate.set("stepSize", Double.toString(stepSize));
				confIntermediate.set("lambda", Double.toString(lambda));
				FileSystem fs = FileSystem.get(confIntermediate);
				
				Job jobIntermediate = new Job(confIntermediate, "DSGD: Intermediate");
	
				FileInputFormat.setInputPaths(jobIntermediate, tempFactorsInput + "," + tempTrainRatings);
				FileOutputFormat.setOutputPath(jobIntermediate, new Path(tempFactorsOutput));
				// Delete the output directory if it exists already
				FileSystem.get(confPreprocRatings).delete(new Path(tempFactorsOutput), true);
				jobIntermediate.setInputFormatClass(SequenceFileInputFormat.class);
				jobIntermediate.setOutputFormatClass(SequenceFileOutputFormat.class);
	
				jobIntermediate.setJarByClass(DSGDMain.class);
				jobIntermediate.setNumReduceTasks(numReducers);
				jobIntermediate.setOutputKeyClass(MatrixItem.class);
				jobIntermediate.setOutputValueClass(NullWritable.class);
				jobIntermediate.setMapOutputKeyClass(IntMatrixItemPair.class);
				jobIntermediate.setMapOutputValueClass(NullWritable.class);
				
				jobIntermediate.setMapperClass(DSGDIntermediateMapper.class);
				// job.setCombinerClass(IntermediateReducer.class);
				jobIntermediate.setReducerClass(DSGDIntermediateReducer.class);
	
				jobIntermediate.waitForCompletion(true);
				
				fs.delete(new Path(tempFactorsInput), true);
				fs.rename(new Path(tempFactorsOutput), new Path(tempFactorsInput));
			}
			/**
			 * jobRmse
			 * Calculate RMSE on test data using current factors
			 */
			timeElapsed += (System.currentTimeMillis() - startTime);
			
			Configuration confRmse = new Configuration();
			confRmse.set("numUsers", numUsers);
			confRmse.set("numReducers", Integer.toString(numBlocks));
			confRmse.set("numItems", numItems);
			confRmse.set("timeElapsed", Long.toString(timeElapsed));
			confRmse.set("kValue", Integer.toString(kValue));
			Job jobRmse = new Job(confRmse, "DSGD: RMSE");
			FileInputFormat.setInputPaths(jobRmse, tempFactorsInput + "," + tempTestRatings);
			FileOutputFormat.setOutputPath(jobRmse, new Path(rmseOutput + "/iter_" + a));
			// Delete the output directory if it exists already
			FileSystem.get(confPreprocRatings).delete(new Path(rmseOutput + "/iter_" + a), true);
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
		Configuration confFinal = new Configuration();
		confFinal.set("kValue", Integer.toString(kValue));
		confFinal.set("numUsers", numUsers);
		confFinal.set("numItems", numItems);
		
		Job jobFinal = new Job(confFinal, "ImprovedPBFS");
		FileInputFormat.setInputPaths(jobFinal, new Path(tempFactorsInput));
		FileOutputFormat.setOutputPath(jobFinal, new Path(finalFactorsOutput));
		// Delete the output directory if it exists already
		FileSystem.get(confPreprocRatings).delete(new Path(finalFactorsOutput), true);

		jobFinal.setInputFormatClass(SequenceFileInputFormat.class);
		jobFinal.setJarByClass(DSGDMain.class);
		jobFinal.setNumReduceTasks(1); // Must use 1 reducer so that both U and M
									   // get sent to same reducer
		
		jobFinal.setOutputKeyClass(Text.class);
		jobFinal.setOutputValueClass(NullWritable.class);
		
		jobFinal.setMapOutputKeyClass(MatrixItem.class);
		jobFinal.setMapOutputValueClass(NullWritable.class);
		
		jobFinal.setMapperClass(DSGDOutputFactorsMapper.class);
		// jobFinal.setCombinerClass(FinalReducer.class);
		jobFinal.setReducerClass(DSGDOutputFactorsReducer.class);

		jobFinal.waitForCompletion(true);
		// fsFinal.delete(tempInputDir, true);
	}

}
