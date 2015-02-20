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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

final public class Matrix {
    private final int M;             // number of rows
    private final int N;             // number of columns
    private final MatrixElement[][] matrix;   // M-by-N array
    private final Map<Integer, MatrixElement> matrixMap;
    
    // create M-by-N matrix of 0's
    public Matrix(int M, int N) {
        this.M = M;
        this.N = N;
        this.matrix = new MatrixElement[M][N];
        this.matrixMap = new HashMap<Integer, MatrixElement>();
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

	public Map<Integer, MatrixElement> getMatrixMap() {
		return matrixMap;
	}

	// create matrix based on 2d array
    public Matrix(MatrixElement[][] data) {
        M = data.length;
        N = data[0].length;
        
        this.matrix = new MatrixElement[M][N];
        this.matrixMap = new HashMap<Integer, MatrixElement>();
        
        int id = 0;
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++) {
            	MatrixElement e = data[i][j];
            	
            	this.matrix[i][j] = e;
            	
            	if(e.getValue() != -1) {
            		this.matrixMap.put(id, e);
            		++id;
            	}
            }
    }

    // copy constructor
    private Matrix(Matrix A) { this(A.matrix); }

    // create and return a random M-by-N matrix with values between 0 and 1
    public static Matrix random(int M, int N) {
        Matrix A = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                A.matrix[i][j].setValue(Math.random());
        return A;
    }

    // create and return the N-by-N identity matrix
    public static Matrix identity(int N) {
        Matrix I = new Matrix(N, N);
        for (int i = 0; i < N; i++)
            I.matrix[i][i].setValue(1);
        return I;
    }

    // swap rows i and j
    public void swap_row(int i, int j) {
        //double[] temp = item[i];
        //item[i] = item[j];
        //item[j] = temp;
        
    	List<Double> temp = new ArrayList<Double>();
    	for(int col = 0; col < N; col++) {
    		temp.add(matrix[i][col].getValue());
    	}
    	
    	for(int col = 0; col < M; col++) {
    		matrix[i][col].setValue(matrix[j][col].getValue());
    	}

    	for(int col = 0; col < M; col++) {
    		matrix[j][col].setValue(temp.get(col));
    	}
    }

    // swap cols i and j
    public void swap_col(int i, int j) {
    	//double[] temp;
    	List<Double> temp = new ArrayList<Double>();
    	//System.out.println("-$-i|j("+i+", "+j+")");
    	for(int row = 0; row < M; row++) {
    		temp.add(matrix[row][i].getValue());
    		//System.out.println("-@-["+item[row][i]+"]");
    	}
    	
    	for(int row = 0; row < M; row++) {
    		matrix[row][i].setValue(matrix[row][j].getValue());
    	}
    	
    	for(int row = 0; row < M; row++) {
    		matrix[row][j].setValue(temp.get(row));
    	}
    }
    
    // create and return the transpose of the invoking matrix
    public Matrix transpose() {
        Matrix A = new Matrix(N, M);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                A.matrix[j][i].setValue(this.matrix[i][j].getValue());
        return A;
    }

    // return C = A + B
    public Matrix plus(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                C.matrix[i][j].setValue(A.matrix[i][j].getValue() + B.matrix[i][j].getValue());
        return C;
    }


    // return C = A - B
    public Matrix minus(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        Matrix C = new Matrix(M, N);
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                C.matrix[i][j].setValue(A.matrix[i][j].getValue() - B.matrix[i][j].getValue());
        return C;
    }

    // does A = B exactly?
    public boolean eq(Matrix B) {
        Matrix A = this;
        if (B.M != A.M || B.N != A.N) throw new RuntimeException("Illegal matrix dimensions.");
        for (int i = 0; i < M; i++)
            for (int j = 0; j < N; j++)
                if (A.matrix[i][j].getValue() != B.matrix[i][j].getValue()) return false;
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
                    C.matrix[i][j].setValue(+(A.matrix[i][k].getValue() * B.matrix[k][j].getValue())); // Need to verify this line
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
                if (Math.abs(A.matrix[j][i].getValue()) > Math.abs(A.matrix[max][i].getValue()))
                    max = j;
            A.swap_row(i, max);
            b.swap_row(i, max);

            // singular
            if (A.matrix[i][i].getValue() == 0.0) throw new RuntimeException("Matrix is singular.");

            // pivot within b
            for (int j = i + 1; j < N; j++)
                b.matrix[j][0].setValue(-(b.matrix[i][0].getValue() * A.matrix[j][i].getValue() / A.matrix[i][i].getValue()));

            // pivot within A
            for (int j = i + 1; j < N; j++) {
                double m = A.matrix[j][i].getValue() / A.matrix[i][i].getValue();
                for (int k = i+1; k < N; k++) {
                    A.matrix[j][k].setValue(-(A.matrix[i][k].getValue() * m));
                }
                A.matrix[j][i].setValue(0.0);
            }
        }

        // back substitution
        Matrix x = new Matrix(N, 1);
        for (int j = N - 1; j >= 0; j--) {
            double t = 0.0;
            for (int k = j + 1; k < N; k++)
                t += A.matrix[j][k].getValue() * x.matrix[k][0].getValue();
            x.matrix[j][0].setValue((b.matrix[j][0].getValue() - t) / A.matrix[j][j].getValue());
        }
        return x;
   
    }

    // print matrix to standard output
    public void print() {
    	
    	DecimalFormat df = new DecimalFormat();
    	df.setMaximumFractionDigits(1);
    	
        for (int i = 0; i < M; i++) {
        	System.out.print("\t");
        	
            for (int j = 0; j < N; j++) {
            	
            	if(matrix[i][j].getValue() != -1) {
            		System.out.print(df.format(matrix[i][j].getValue())+"\t");
            		
            	} else if(i == 0 || j == 0) {
            		if(i == 0 && j == 0)
            			System.out.print("X\t");
            		else
            			System.out.print(Math.round(matrix[i][j].getValue())+"\t");
            		
            	} else            	
            		System.out.print("X\t");
            }
            
            System.out.println();
        }
    }
    
    public MatrixElement getMatrixElement(int id) {
    	return (matrixMap.get(id));
    }
    
    // Find the Max element from the posXpos sub Matrix
    public MatrixElement findMax(int pos) {
    	double max = 0;
    	MatrixElement e = null;
    	
        for (int row = pos; row < this.getM(); row++) {
            for (int col = pos; col < this.getN(); col++) {
            	//System.out.println("-#-r"+row+"c"+col);
            	if(max <= matrix[row][col].getValue()) {
            		max = matrix[row][col].getValue();
            		e = matrix[row][col];
            	}
            }            	            	            
        }
                    	
    	return e;
    }
    
    // Returns a Set of Max element
    public Set<MatrixElement> findMaxSet(int pos) {
    	double max = 0;
    	TreeSet<MatrixElement> eSet = new TreeSet<MatrixElement>();
    	MatrixElement e = null;
    	
        for (int row = pos; row < this.getM(); row++) {
            for (int col = pos; col < this.getN(); col++) {
            	//System.out.println("-#-r"+row+"c"+col);
            	if(max <= matrix[row][col].getValue()) {
            		max = matrix[row][col].getValue();
            		e = matrix[row][col];
            		eSet.add(e);
            	}
            }            	            	            
        }

    	return eSet;
    }

    // Returns a Sorted List of Matrix elements
    public ArrayList<MatrixElement> getSortedValueSet(int pos) {
    	ArrayList<MatrixElement> eSet = new ArrayList<MatrixElement>();
    	Map<Integer, MatrixElement> eMap = new HashMap<Integer, MatrixElement>();
    	MatrixElement e = null;
    	
        for (int row = pos; row < this.getM(); row++) {
            for (int col = pos; col < this.getN(); col++) {            	
            	e = matrix[row][col];
            		
            	if(e.getValue() != -1)
            		eMap.put(e.getId(), e);            	
            }            	            	            
        }

        eSet.addAll(eMap.values());
        Collections.sort(eSet);
        
    	return eSet;
    }    
    
    // Find Max counts in a specific column of the Matrix
    // In case of multiple max values in a Column this function will return all of them with their corresponding row id (i.e Partition id)
    public Map<Integer, MatrixElement> findColMaxSet(int col) {
    	double max = 0;
    	Map<Integer, MatrixElement> e_max = new HashMap<Integer, MatrixElement>();
    	
    	for(int row = 1; row < this.getM(); row++) {
    		//System.out.println("-#-r"+row+"c"+col);
    		if(max <= matrix[row][col].getValue()) {
    			max = matrix[row][col].getValue();
    			e_max.put(row, matrix[row][col]);
    		}
    	}
    	
    	return e_max;
    }
    
    // Find Max counts in a specific column of the Matrix
    public MatrixElement findColMax(int col) {
    	double max = 0;
    	MatrixElement e = null;
    	
    	for(int row = 1; row < this.getM(); row++) {
    		//System.out.println("-#-r"+row+"c"+col);
    		if(max <= matrix[row][col].getValue()) {
    			max = matrix[row][col].getValue();
    			e = matrix[row][col];
    		}
    	}
    	
    	return e;
    }
    
    // Find Max counts in a specific row of the Matrix
    public MatrixElement findRowMax(int row) {
    	double max = 0;
    	MatrixElement e = null;
    	
    	for(int col = 1; col < this.getN(); col++) {
    		if(max < matrix[row][col].getValue()) {
    			max = matrix[row][col].getValue();
    			e = matrix[row][col];
    		}
    	}
    	
    	return e;
    }
}