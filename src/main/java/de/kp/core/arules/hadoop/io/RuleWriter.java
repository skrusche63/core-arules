package de.kp.core.arules.hadoop.io;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import de.kp.core.arules.RedBlackTree;
import de.kp.core.arules.RuleG;

public class RuleWriter {

	@SuppressWarnings("rawtypes")
	public void write(PriorityQueue<RuleG> rules, String output, JobConf jobConf) throws IOException {

		Tap tap = new Hfs(new TextLine(new Fields("rule")),output, SinkMode.REPLACE);		
	    TupleEntryCollector collector = new HadoopFlowProcess(jobConf).openTapForWrite(tap);
        
	    /*
	     * Sort the rules in sorted order before printing them
		 * because the Iterator from Java on a priority queue do not
		 * show the rules in priority order unfortunately (even though
		 * they are sorted in the priority queue. 
		 */
		Object[] ary = rules.toArray();
		Arrays.sort(ary);  
		
		// for each rule
		for(Object ruleObj : ary){
			RuleG rule = (RuleG) ruleObj;
			
			// Write the rule
			StringBuffer buffer = new StringBuffer();
			buffer.append(rule.toString());
			// write separator
			buffer.append(" #SUP: ");
			// write support
			buffer.append(rule.getAbsoluteSupport());
			// write separator
			buffer.append(" #CONF: ");
			// write confidence
			buffer.append(rule.getConfidence());

			String line = buffer.toString();

			Fields fields = new Fields("rule");
		    Class<?>[] types = new Class<?>[] {String.class};

		    TupleEntry entry = new TupleEntry(fields, new Tuple(new Object[types.length]));			
			entry.setString("rule",line);
			
			collector.add(entry);

		}

	    collector.close();
		
	}
	
	@SuppressWarnings({ "rawtypes"})
	public void write(RedBlackTree<RuleG> rules, String output, JobConf jobConf) throws IOException {

		Tap tap = new Hfs(new TextLine(new Fields("rule")),output, SinkMode.REPLACE);		
	    TupleEntryCollector collector = new HadoopFlowProcess(jobConf).openTapForWrite(tap);

		Iterator<RuleG> iter = rules.iterator();
		while (iter.hasNext()) {
			// Write the rule
			RuleG rule = (RuleG) iter.next();
			StringBuffer buffer = new StringBuffer();
			buffer.append(rule.toString());
			// write separator
			buffer.append(" #SUP: ");
			// write support
			buffer.append(rule.getAbsoluteSupport());
			// write confidence
			buffer.append(" #CONF: ");
			buffer.append(rule.getConfidence());

			String line = buffer.toString();

			Fields fields = new Fields("rule");
		    Class<?>[] types = new Class<?>[] {String.class};

		    TupleEntry entry = new TupleEntry(fields, new Tuple(new Object[types.length]));			
			entry.setString("rule",line);
			
			collector.add(entry);
			
		}
	    collector.close();
	}

}
