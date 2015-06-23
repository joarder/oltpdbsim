/*******************************************************************************
 * Copyright [2014] [Joarder Kamal]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package main.java.utils;

public class MatrixElement implements Comparable<MatrixElement> {
	private int id;
	private int row_pos;
	private int col_pos;
	private double value;
	
	public MatrixElement(int id, int r, int c, double val) {
		this.setId(id);
		this.setRow_pos(r);
		this.setCol_pos(c);
		this.setValue(val);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getRow_pos() {
		return row_pos;
	}

	public void setRow_pos(int row_pos) {
		this.row_pos = row_pos;
	}

	public int getCol_pos() {
		return col_pos;
	}

	public void setCol_pos(int col_pos) {
		this.col_pos = col_pos;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public boolean equals(Object object) {
		if (!(object instanceof MatrixElement)) {
			return false;
		}
		
		MatrixElement e = (MatrixElement) object;
		return (this.getId() == e.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.id;
		return result;
	}

	@Override
	public int compareTo(MatrixElement e) {		
		return (((double)this.getValue() > (double)e.getValue()) ? -1 : 
			((double)this.getValue() < (double)e.getValue()) ? 1 : 0);	
	}
	
	@Override
	public String toString() {		
		return ("("+this.getRow_pos()+", "+this.getCol_pos()+") | association = "+this.getValue());
	}
}