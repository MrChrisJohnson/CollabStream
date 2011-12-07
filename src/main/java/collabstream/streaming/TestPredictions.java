package collabstream.streaming;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;

public class TestPredictions {
	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
			System.err.println("######## Wrong number of arguments");
			System.err.println("######## required args: numUsers numItems numLatent"
				+ " trainingFilename testFilename userFilename itemFilename");
			return;
		}
		
		long testStartTime = System.currentTimeMillis();
		System.out.printf("######## Testing started: %1$tY-%1$tb-%1$td %1$tT %tZ\n", testStartTime);
		
		int numUsers = Integer.parseInt(args[0]);
		int numItems = Integer.parseInt(args[1]);
		int numLatent = Integer.parseInt(args[2]);
		String trainingFilename = args[3];
		String testFilename = args[4];
		String userFilename = args[5];
		String itemFilename = args[6];

		float trainingTotal = 0.0f;
		int trainingCount = 0;
		
		Map<Integer, Integer> userCount = new HashMap<Integer, Integer>();
		Map<Integer, Integer> itemCount = new HashMap<Integer, Integer>();
		Map<Integer, Float> userTotal = new HashMap<Integer, Float>();
		Map<Integer, Float> itemTotal = new HashMap<Integer, Float>();
		
		long startTime = System.currentTimeMillis();
		System.out.printf("######## Started reading training file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", startTime);

		String line;
		LineNumberReader in = new LineNumberReader(new FileReader(trainingFilename));
		while ((line = in.readLine()) != null) {
			try {
				String[] token = StringUtils.split(line, ' ');
				int i = Integer.parseInt(token[0]);
				int j = Integer.parseInt(token[1]);
				float rating = Float.parseFloat(token[2]);
				
				trainingTotal += rating;
				++trainingCount;
				
				if (userCount.containsKey(i)) {
					userCount.put(i, userCount.get(i) + 1);
					userTotal.put(i, userTotal.get(i) + rating);
				} else {
					userCount.put(i, 1);
					userTotal.put(i, rating);
				}
				
				if (itemCount.containsKey(j)) {
					itemCount.put(j, itemCount.get(j) + 1);
					itemTotal.put(j, itemTotal.get(j) + rating);
				} else {
					itemCount.put(j, 1);
					itemTotal.put(j, rating);
				}
			} catch (Exception e) {
				System.err.printf("######## Could not parse line %d in %s\n%s\n", in.getLineNumber(), trainingFilename, e);
			}
		}
		in.close();
		
		float trainingAvg = trainingTotal / trainingCount;
		
		long endTime = System.currentTimeMillis();
		System.out.printf("######## Finished reading training file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", endTime);
		System.out.println("######## Time elapsed reading training file: "
			+ DurationFormatUtils.formatPeriod(startTime, endTime, "H:m:s") + " (h:m:s)");
		
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
		
		startTime = System.currentTimeMillis();
		System.out.printf("######## Started reading user file: %1$tY-%1$tb-%1$td %1$tT %tZ\n", startTime);
		
		in = new LineNumberReader(new FileReader(userFilename));
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
		
		endTime = System.currentTimeMillis();
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
				float prediction;
				
				boolean userKnown = userCount.containsKey(i);
				boolean itemKnown = itemCount.containsKey(j);
				
				if (userKnown && itemKnown) {
					prediction = 0.0f;
					for (int k = 0; k < numLatent; ++k) {
						prediction += userMatrix[i][k] * itemMatrix[j][k];
					}
				} else if (userKnown) {
					prediction = userTotal.get(i) / userCount.get(i);
				} else if (itemKnown) {
					prediction = itemTotal.get(j) / itemCount.get(j);
				} else {
					prediction = trainingAvg;
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
		System.out.println("######## Number of ratings used: " + numRatings);
		System.out.println("######## RMSE: " + rmse);
	}
}