package collabstream.streaming;

import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.lang.time.DurationFormatUtils;

public class PermutationUtils {
	private static final ThreadLocal<Random> localRandom = new ThreadLocal<Random>() {
		protected Random initialValue() { return new Random(); }
	};

	public static <T> T[] permute(T[] arr) {
		if (arr == null) return null;
		Random random = localRandom.get();
		for (int n = arr.length; n > 1; --n) {
			int i = random.nextInt(n);
			T temp = arr[i];
			arr[i] = arr[n-1];
			arr[n-1] = temp;
		}
		return arr;
	}
	
	public static <T> ArrayList<T> permute(ArrayList<T> list) {
		if (list == null) return null;
		Random random = localRandom.get();
		for (int n = list.size(); n > 1; --n) {
			int i = random.nextInt(n);
			T temp = list.get(i);
			list.set(i, list.get(n-1));
			list.set(n-1, temp);
		}
		return list;		
	}
}