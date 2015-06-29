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
/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package main.java.workload.twitter;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import ch.ethz.ssh2.util.Tokenizer;

public class TransactionSelector {

	String filename, filename2;
	DataInputStream dis, dis2 = null;	
	
	// http://www.globule.org/publi/WWADH_comnet2009.html
	static final double READ_WRITE_RATIO = 11.8;
												
	public TransactionSelector(String filename, String filename2) throws FileNotFoundException {

		this.filename = filename;
		this.filename2 = filename2;

		if(filename==null || filename.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the TransactionSelector... (probably missing in your workload configuration?)");

		if(filename2==null || filename2.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the TransactionSelector... (probably missing in your workload configuration?)");
		
		File file = new File(filename);
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		fis = new FileInputStream(file);

		// Here BufferedInputStream is added for fast reading.
		bis = new BufferedInputStream(fis);
		dis = new DataInputStream(bis);
		dis.mark(1024 * 1024 * 1024);

		File file2 = new File(filename2);
		FileInputStream fis2 = null;
		BufferedInputStream bis2 = null;
		fis2 = new FileInputStream(file2);

		// Here BufferedInputStream is added for fast reading.
		bis2 = new BufferedInputStream(fis2);
		dis2 = new DataInputStream(bis2);
		dis2.mark(1024 * 1024 * 1024);	
	}

	public synchronized TwitterOperation nextTransaction() throws IOException {
		if (dis.available() == 0)
			dis.reset();
		
		if (dis2.available() == 0)
			dis2.reset();

		return readNextTransaction();
	}

	private TwitterOperation readNextTransaction() throws IOException {
		String line = dis.readUTF(); //readLine();
		String[] sa = Tokenizer.parseTokens(line, ' ');
		int tweet_id = Integer.parseInt(sa[0]);

		String line2 = dis2.readUTF(); //readLine();
		String[] sa2 = Tokenizer.parseTokens(line2, ' ');
		int user_id = Integer.parseInt(sa2[0]);
		
		return new TwitterOperation(user_id, tweet_id);
	}

	public ArrayList<TwitterOperation> readAll() throws IOException {
		ArrayList<TwitterOperation> transactions = new ArrayList<TwitterOperation>();

		while (dis.available() > 0 && dis2.available() > 0) {
			transactions.add(readNextTransaction());
		}

		return transactions;
	}

	public void close() throws IOException {
		dis.close();
		dis2.close();
	}
}
