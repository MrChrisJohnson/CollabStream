package collabstream.streaming;

import java.io.FileReader;
import java.io.LineNumberReader;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;

public class TestPredictions {
	public static void main(String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("######## Wrong number of arguments");
			System.err.println("######## required args: numUsers numItems numLatent testFilename userFilename itemFilename");
			return;
		}
		
		long testStartTime = System.currentTimeMillis();
		System.out.printf("######## Testing started: %1$tY-%1$tb-%1$td %1$tT %tZ\n", testStartTime);
		
		int numUsers = Integer.parseInt(args[0]);
		int numItems = Integer.parseInt(args[1]);
		int numLatent = Integer.parseInt(args[2]);
		String testFilename = args[3];
		String userFilename = args[4];
		String itemFilename = args[5];
		
		float[][] userMatrix = new float[numUsers][numLatent];
		for (int i = 0; i < numUsers; ++i) {
			for (int k = 0; k < numLatent; ++k) {
				userMatrix[i][k] = 0.0f;
			}
		}
		
		float[][] itemMatrix = new float[numItems][numLatent];
		for (int i = 0; i < numItems; ++i) {
			for (int k = 0; k < numLatent; ++k) {
				itemMatrix[i][k] = 0.0f;
			}
		}
		
		String line;
		
		long startTime = System.currentTimeMillis();
		System.out.printf("######## Started reading user file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", startTime);
		
		LineNumberReader in = new LineNumberReader(new FileReader(userFilename));
		while ((line = in.readLine()) != null) {
			try {
				String[] token = StringUtils.split(line, ' ');
				int i = Integer.parseInt(token[0]);
				for (int k = 0; k < numLatent; ++k) {
					userMatrix[i][k] = Float.parseFloat(token[k+1]);
				}
			} catch (Exception e) {
				System.err.printf("######## Could not parse line %d in %s\n%s\n", in.getLineNumber(), userFilename, e);
			}
		}
		in.close();
		
		long endTime = System.currentTimeMillis();
		System.out.printf("######## Finished reading user file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", endTime);
		System.out.println("######## Time elapsed reading user file: "
			+ DurationFormatUtils.formatPeriod(startTime, endTime, "H:m:s") + " (h:m:s)");
		
		startTime = System.currentTimeMillis();
		System.out.printf("######## Started reading item file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", startTime);
		
		in = new LineNumberReader(new FileReader(itemFilename));
		while ((line = in.readLine()) != null) {
			try {
				String[] token = StringUtils.split(line, ' ');
				int j = Integer.parseInt(token[0]);
				for (int k = 0; k < numLatent; ++k) {
					itemMatrix[j][k] = Float.parseFloat(token[k+1]);
				}
			} catch (Exception e) {
				System.err.printf("######## Could not parse line %d in %s\n%s\n", in.getLineNumber(), itemFilename, e);
			}
		}
		in.close();
		
		endTime = System.currentTimeMillis();
		System.out.printf("######## Finished reading item file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", endTime);
		System.out.println("######## Time elapsed reading item file: "
			+ DurationFormatUtils.formatPeriod(startTime, endTime, "H:m:s") + " (h:m:s)");
		
		startTime = System.currentTimeMillis();
		System.out.printf("######## Started reading test file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", startTime);
		
		float totalSqErr = 0.0f;
		int numRatings = 0;
		
		in = new LineNumberReader(new FileReader(testFilename));
		while ((line = in.readLine()) != null) {
			try {
				String[] token = StringUtils.split(line, ' ');
				int i = Integer.parseInt(token[0]);
				int j = Integer.parseInt(token[1]);
				float rating = Float.parseFloat(token[2]);
				
				float prediction = 0.0f;
				for (int k = 0; k < numLatent; ++k) {
					prediction += userMatrix[i][k] * itemMatrix[j][k];
				}
				
				float diff = prediction - rating;
				totalSqErr += diff*diff;
				++numRatings;
			} catch (Exception e) {
				System.err.printf("######## Could not parse line %d in %s\n%s\n", in.getLineNumber(), testFilename, e);
			}
		}
		
		double rmse = Math.sqrt(totalSqErr / numRatings);
		
		endTime = System.currentTimeMillis();
		System.out.printf("######## Finished reading test file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", endTime);
		System.out.println("######## Time elapsed reading test file: "
			+ DurationFormatUtils.formatPeriod(startTime, endTime, "H:m:s") + " (h:m:s)");
		System.out.println("######## Total elapsed testing time: "
			+ DurationFormatUtils.formatPeriod(testStartTime, endTime, "H:m:s") + " (h:m:s)");
		System.out.println("######## RMSE: " + rmse);
	}
}