/*************************************************************************
 *  Compilation:  javac Matrix.java
 *  Execution:    java Matrix
 *
 *  A bare-bones immutable data type for M-by-N matrices.
 *  
 *  Collected from : http://introcs.cs.princeton.edu/95linear/Matrix.java.html
 *
 *************************************************************************/

package main.java.utils;

import java.util.ArrayList;
import java.util.List;

final public class Matrix {
    private final int M;             // number of rows
    private final int N;             // number of columns
    private final MatrixElement[][] matrix;   // M-by-N array
    
    // create M-by-N matrix of 0's
    public Matrix(int M, int N) {
        this.M = M;
        this.N = N;
        this.matrix = new MatrixElement[M][N];
    }

    public int getM() {
		return M;
	}

	public int getN() {
		return N;
	}

	public MatrixElement[][] getMatrix() {
		return matrix;
	}

	// create matrix based on 2d array
    public Matrix(MatrixElement[][] data) {
        M = data.length;
        N = data[0].length;
        this.matrix = new MatrixElement[M][N];
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                    this.matrix[i][j] = data[i][j];
    }

    // copy constructor
    private Matrix(Matrix A) { this(A.matrix); }

    // create and return a random M-by-N matrix with values between 0 and 1
    public static Matrix random(int M, int N) {
        Matrix A = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                A.matrix[i][j].setCounts(Math.random());
        return A;
    }

    // create and return the N-by-N identity matrix
    public static Matrix identity(int N) {
        Matrix I = new Matrix(N, N);
        for (int i = 0; i < N; i++)
            I.matrix[i][i].setCounts(1);
        return I;
    }

    // swap rows i and j
    public void swap_row(int i, int j) {
        //double[] temp = item[i];
        //item[i] = item[j];
        //item[j] = temp;
        
    	List<Double> temp = new ArrayList<Double>();
    	for(int col = 0; col < N; col++) {
    		temp.add(matrix[i][col].getCounts());
    	}
    	
    	for(int col = 0; col < M; col++) {
    		matrix[i][col].setCounts(matrix[j][col].getCounts());
    	}

    	for(int col = 0; col < M; col++) {
    		matrix[j][col].setCounts(temp.get(col));
    	}
    }

    // swap cols i and j
    public void swap_col(int i, int j) {
    	//double[] temp;
    	List<Double> temp = new ArrayList<Double>();
    	//System.out.println("-$-i|j("+i+", "+j+")");
    	for(int row = 0; row < M; row++) {
    		temp.add(matrix[row][i].getCounts());
    		//System.out.println("-@-["+item[row][i]+"]");
    	}
    	
    	for(int row = 0; row < M; row++) {
    		matrix[row][i].setCounts(matrix[row][j].getCounts());
    	}
    	
    	for(int row = 0; row < M; row++) {
    		matrix[row][j].setCounts(temp.get(row));
    	}
    }
    
    // create and return the transpose of the invoking matrix
    public Matrix transpose() {
        Matrix A = new Matrix(N, M);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                A.matrix[j][i].setCounts(this.matrix[i][j].getCounts());
        return A;
    }

    // return C = A + B
    public Matrix plus(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                C.matrix[i][j].setCounts(A.matrix[i][j].getCounts() + B.matrix[i][j].getCounts());
        return C;
    }


    // return C = A - B
    public Matrix minus(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                C.matrix[i][j].setCounts(A.matrix[i][j].getCounts() - B.matrix[i][j].getCounts());
        return C;
    }

    // does A = B exactly?
    public boolean eq(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                if (A.matrix[i][j].getCounts() != B.matrix[i][j].getCounts()) return false;
        return true;
    }

    // return C = A * B
    public Matrix times(Matrix B) {
        Matrix A = this;
        if (A.N != B.M) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(A.M, B.N);
        for (int i = 0; i < C.M; i++)
            for (int j = 0; j < C.N; j++)
                for (int k = 0; k < A.N; k++)
                    C.matrix[i][j].setCounts(+(A.matrix[i][k].getCounts() * B.matrix[k][j].getCounts())); // Need to verify this line
        return C;
    }


    // return x = A^-1 b, assuming A is square and has full rank
    public Matrix solve(Matrix rhs) {
        if (M != N || rhs.M != N || rhs.N != 1)
            throw new RuntimeException("Illegal matrix dimensions.");

        // create copies of the data
        Matrix A = new Matrix(this);
        Matrix b = new Matrix(rhs);

        // Gaussian elimination with partial pivoting
        for (int i = 0; i < N; i++) {

            // find pivot row and swap
            int max = i;
            for (int j = i + 1; j < N; j++)
                if (Math.abs(A.matrix[j][i].getCounts()) > Math.abs(A.matrix[max][i].getCounts()))
                    max = j;
            A.swap_row(i, max);
            b.swap_row(i, max);

            // singular
            if (A.matrix[i][i].getCounts() == 0.0) throw new RuntimeException("Matrix is singular.");

            // pivot within b
            for (int j = i + 1; j < N; j++)
                b.matrix[j][0].setCounts(-(b.matrix[i][0].getCounts() * A.matrix[j][i].getCounts() / A.matrix[i][i].getCounts()));

            // pivot within A
            for (int j = i + 1; j < N; j++) {
                double m = A.matrix[j][i].getCounts() / A.matrix[i][i].getCounts();
                for (int k = i+1; k < N; k++) {
                    A.matrix[j][k].setCounts(-(A.matrix[i][k].getCounts() * m));
                }
                A.matrix[j][i].setCounts(0.0);
            }
        }

        // back substitution
        Matrix x = new Matrix(N, 1);
        for (int j = N - 1; j >= 0; j--) {
            double t = 0.0;
            for (int k = j + 1; k < N; k++)
                t += A.matrix[j][k].getCounts() * x.matrix[k][0].getCounts();
            x.matrix[j][0].setCounts((b.matrix[j][0].getCounts() - t) / A.matrix[j][j].getCounts());
        }
        return x;
   
    }

    // print matrix to standard output
    public void print() {
        for (int i = 0; i < M; i++) {
        	System.out.print("\t");
            for (int j = 0; j < N; j++) {
            	if(matrix[i][j].getCounts() != -1)
            		System.out.print(Math.round(matrix[i][j].getCounts())+"\t");
            	else            	
            		System.out.print("X\t");
            }
            System.out.println();
        }
    }
    
    // Find the Max element from the posXpos sub Matrix
    public MatrixElement findMax(int pos) {
    	double max_counts = 0;
    	MatrixElement e = null;
    	
        for (int row = pos; row < this.getM(); row++) {
            for (int col = pos; col < this.getN(); col++) {
            	//System.out.println("-#-r"+row+"c"+col);
            	if(max_counts <= matrix[row][col].getCounts()) {
            		max_counts = matrix[row][col].getCounts();
            		e = matrix[row][col];
            	}
            }            	            	            
        }
                    	
    	return e;
    }
    
    // Find Max counts in a specific column of the Matrix
    public MatrixElement findColMax(int col) {
    	double max_counts = 0;
    	MatrixElement e = null;
    	
    	for(int row = 1; row < this.getM(); row++) {
    		//System.out.println("-#-r"+row+"c"+col);
    		if(max_counts <= matrix[row][col].getCounts()) {
    			max_counts = matrix[row][col].getCounts();
    			e = matrix[row][col];
    		}
    	}
    	
    	return e;
    }
    
    // Find Max counts in a specific row of the Matrix
    public MatrixElement findRowMax(int row) {
    	double max_counts = 0;
    	MatrixElement e = null;
    	
    	for(int col = 1; col < this.getN(); col++) {
    		if(max_counts < matrix[row][col].getCounts()) {
    			max_counts = matrix[row][col].getCounts();
    			e = matrix[row][col];
    		}
    	}
    	
    	return e;
    }
}