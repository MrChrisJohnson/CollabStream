package comparison.dsgd;

public class MatrixUtils {

	public static double dotProduct(double[] v1, double[] v2) throws MatrixException{
		if(v1.length != v2.length){
			throw new MatrixException();
		}
		
		double total = 0;
		for(int i = 0; i < v1.length; ++i){
			total += v1[i]*v2[i];
		}
		
		return total;
	}
	
	public static class MatrixException extends Exception{

		private static final long serialVersionUID = 6678191521290159097L;

		
	}
}
