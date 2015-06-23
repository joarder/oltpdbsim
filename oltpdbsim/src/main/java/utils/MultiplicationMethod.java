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
/** Implements the multiplication method of hashing on pages 231-232
 * of <i>Introduction to Algorithms</i>, Second edition. */

package main.java.utils;

public class MultiplicationMethod
{
    /** The size of the hash table being used. */
    private int tableSize;

    /** <code>true</code> if the table size is a power of 2,
     * <code>false</code> otherwise. */
    private boolean isPowerOf2;

    /** If the table size is a power of 2, the shift amount used. */
    private int shiftAmount;

    /** If the table size is a power of 2, the bit mask used. */
    private long bitMask;

    /**
     * Creates a hash function that uses the multiplication method.
     *
     * @param size The size of the hash table.
     */
    public MultiplicationMethod(int size)
    {
	tableSize = size;
	isPowerOf2 = (size & (size-1)) == 0;

	if (isPowerOf2) {
	    // Determine the lg of the table size, denoted by p.
	    int p = 0;
	    int x = 1;
	    while (x < size) {
		x *= 2;
		p++;
	    }

	    shiftAmount = 32 - p;
	    bitMask = size - 1;
	}
    }

    /**
     * Returns the hash value of an object, based on its Java
     * <code>hashCode</code> value and the multiplication method.
     *
     * @param o The object being hashed.  If the object implements
     * <code>DynamicSetElement</code>, the hash value is of its key.
     */
    public int hash(Object o)
    {
	// If the object implements <code>DynamicSetElement</code>,
	// hash its key.  Otherwise, just hash the object itself.
	//if (o instanceof DynamicSetElement)
	   // o = ((DynamicSetElement) o).getKey();

	// If the table size is a power of 2, we can use the faster
	// method shown in Figure 11.4 on page 232.
	if (isPowerOf2) {
	    final long S = 2654435769L;

	    int k = o.hashCode();
	    long r = S * k;

	    return (int) ((r >> shiftAmount) & bitMask);
	    }
	else {
	    // Use the less efficient method, which uses
	    // floating-point arithmetic.

	    // The constant used in the multiplication method.
	    final double MULTIPLIER = 0.6180339887;
	    
	    double x = o.hashCode() * MULTIPLIER;
	    x -= Math.floor(x);	// take the fractional part
	    x *= tableSize;	// multiply by the table size
	    return (int) x;	// return the integer version
	}
    }
}