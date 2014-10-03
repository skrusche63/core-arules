package de.kp.core.arules;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class Vertical implements Serializable {

	private static final long serialVersionUID = -2241849627931027024L;

	/*
	 * The max value of all the items associated with the
	 * transactions under consideration
	 */
	public int max;
	
	/*
	 * A vertical representation of the database
	 */
	public BitSet[] tableItemTids; // [item], IDs of transaction containing the item	
	/*
	 * A table indicating the support of each item
	 */
	public int[] tableItemCount; // [item], support

	public List<Transaction> transactions;
	
	public Vertical() {
		this.transactions = new ArrayList<Transaction>();
	}
	
	public Vertical(int size) {
		initialize(size);		
	}
	
	public Vertical(BitSet[] tableItemTids, int[] tableItemCount,Transaction[] transactions,int max) {
		
		this.tableItemTids  = tableItemTids;
	    this.tableItemCount = tableItemCount;
	    
	    this.transactions = Arrays.asList(transactions);
	    
	    this.max = max;
	    
	}
	
	public void setSize(int size) {
		
		if (tableItemTids == null || tableItemCount == null) {

			/* 
			 * Initialize data structures
			 */
			initialize(size);
			
		}
		
	}
	
	public void setTrans(Transaction trans) {
		this.transactions.add(trans);
	}
	
	private void initialize(int size) {
		
		/* 
		 * Initialize data structures; with this initialization, items
		 * from 1..max are supported; note, that there is position [0],
		 * that is not used here
		 */
		tableItemTids  = new BitSet[size + 1];
		tableItemCount = new int[size + 1];

		/*
		 * Additional initialization due to the counting
		 * of the respective items: item '0' does not exist
		 */
		tableItemTids[0] = new BitSet();
		tableItemCount[0] = 0;
		
		max = size;

	}
	
}
