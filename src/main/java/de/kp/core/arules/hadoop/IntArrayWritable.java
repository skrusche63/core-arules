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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable implements Writable {

	public ArrayWritable intArrayWritable;
	
	public IntArrayWritable() {
		
		List<IntWritable> empty = Collections.<IntWritable> emptyList();		
		this.intArrayWritable = new ArrayWritable(IntWritable.class, empty.toArray(new Writable[empty.size()]));

	}
	
	public IntArrayWritable(int[] intArray) {
		
		ArrayList<IntWritable> items = new ArrayList<IntWritable>();
		for (int i=0; i < intArray.length; i++) {
			items.add(new IntWritable(intArray[i]));
		}

		this.intArrayWritable = new ArrayWritable(IntWritable.class, items.toArray(new Writable[items.size()]));

	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		intArrayWritable.readFields(in);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		intArrayWritable.write(out);		
	}
	
	public int[] get() {
		
		Writable[] writableArray = intArrayWritable.get();
		
		int[] intArray = new int[writableArray.length];
		for (int i=0; i < writableArray.length; i++) {
			
			IntWritable item = (IntWritable) writableArray[i];			
			intArray[i] = item.get();
			
		}
		
		return intArray;
		
	}

}
