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
import java.util.List;

import org.apache.hadoop.io.Writable;

import de.kp.core.arules.Transaction;

public class TransactionWritable implements Writable {

	private Transaction transaction;
	
	public TransactionWritable() {		
	}
	
	public TransactionWritable(Transaction transaction) {
		this.transaction = transaction;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int tid = in.readInt();
		int size = in.readInt();
		
		this.transaction = new Transaction(size);
		this.transaction.setId(String.valueOf(tid));
		
		for (int i=0; i < size; i++) {
			int item = in.readInt();
			this.transaction.addItem(item);
		}
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		String tid = transaction.getId();
		out.writeInt(Integer.parseInt(tid));
		
		List<Integer> items = transaction.getItems();
		out.writeInt(items.size());
		
		for (Integer item : items) {
			out.writeInt(item);
		}
		
	}

	public Transaction get() {
		return this.transaction;
	}
	
}
