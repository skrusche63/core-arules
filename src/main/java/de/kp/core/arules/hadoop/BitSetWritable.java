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
import java.util.BitSet;

import org.apache.hadoop.io.Writable;

public class BitSetWritable implements Writable {

	private BitSet bitset;

	public BitSetWritable() {
	}

	public BitSetWritable(BitSet set) {
		this.bitset = set;
	}

	public BitSet get() {
		return this.bitset;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {

		long[] longs = new long[in.readInt()];
		for (int i = 0; i < longs.length; i++) {
			longs[i] = in.readLong();
		}

		bitset = BitSet.valueOf(longs);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		
		long[] longs = bitset.toLongArray();
		out.writeInt(longs.length);
		
		for (int i = 0; i < longs.length; i++) {
			out.writeLong(longs[i]);
		}
	
	}

}
