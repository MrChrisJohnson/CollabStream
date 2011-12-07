package comparison.dsgd;

public class MatrixUtils {

	public static double dotProduct(double[] v1, double[] v2){
		if(v1.length != v2.length){
			System.out.println("lengths do not match!!");
			return 0;
		}
		
		double total = 0;
		for(int i = 0; i < v1.length; ++i){
//			System.out.println("v1{" + i + "]: " + v1[i]);
//			System.out.println("v2{" + i + "]: " + v2[i]);
			total += v1[i]*v2[i];
		}
		
		return total;
	}
	
	public static class MatrixException extends Exception{

		private static final long serialVersionUID = 6678191521290159097L;

		
	}
}
