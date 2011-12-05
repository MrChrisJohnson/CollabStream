package comparison.dsgd;

public class LongPair {

	public long first;
	public long second;

	public LongPair(long first, long second) {
		this.first = first;
		this.second = second;
	}

	public int compareTo(LongPair o) {
		if (this.first != o.first) {
			if (this.first < o.first) {
				return -1;
			} else {
				return 1;
			}
		} else if (this.second != o.second) {
			if (this.second < o.second) {
				return -1;
			} else {
				return 1;
			}
		}
		return 0;
	}
	
}
