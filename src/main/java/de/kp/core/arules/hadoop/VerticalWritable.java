package de.kp.core.arules.hadoop;
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Core-ARULES project
* (https://github.com/skrusche63/core-arules).
* 
* Core-ARULES is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Core-ARULES is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Core-ARULES. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import de.kp.core.arules.Vertical;

public class VerticalWritable implements Writable {

	/*
	 * The max value of all the items associated with the
	 * transactions under consideration
	 */
	private IntWritable max;
	/*
	 * A vertical representation of the database
	 */
	public BitSetArrayWritable tableItemTids; // [item], IDs of transaction containing the item	
	/*
	 * A table indicating the support of each item
	 */
	public IntArrayWritable tableItemCount; // [item], support	
	public TransactionListWritable transactions;
	
	public VerticalWritable() {	
		
		max = new IntWritable();
		
		tableItemTids = new BitSetArrayWritable();
		tableItemCount = new IntArrayWritable();
		
		transactions = new TransactionListWritable();
		
	}
	
	public VerticalWritable(Vertical vertical) {
		
		max = new IntWritable(vertical.max);
		
		tableItemTids = new BitSetArrayWritable(vertical.tableItemTids);
		tableItemCount = new IntArrayWritable(vertical.tableItemCount);
		
		transactions = new TransactionListWritable(vertical.transactions);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		max.readFields(in);
		
		tableItemTids.readFields(in);
		tableItemCount.readFields(in);
		
		transactions.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		max.write(out);
		
		tableItemTids.write(out);
		tableItemCount.write(out);
		
		transactions.write(out);
		
	}

	public Vertical get() {
		
		int max = this.max.get(); 		
		Vertical vertical = new Vertical(max);
		
		vertical.tableItemCount = this.tableItemCount.get();
		vertical.tableItemTids  = this.tableItemTids.get();
		
		vertical.transactions = this.transactions.get();
		
		return vertical;
		
	}
}


