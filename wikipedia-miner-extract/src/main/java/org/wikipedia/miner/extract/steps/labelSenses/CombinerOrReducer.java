package org.wikipedia.miner.extract.steps.labelSenses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.wikipedia.miner.extract.model.struct.LabelSense;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;


public abstract class CombinerOrReducer extends AvroReducer<CharSequence, LabelSenseList, Pair<CharSequence, LabelSenseList>> {
	
	public enum Counts {ambiguous, unambiguous} ;
	
	public abstract boolean isReducer() ;
	
	
	
	
	@Override
	public void reduce(CharSequence label, Iterable<LabelSenseList> senseLists,
			AvroCollector<Pair<CharSequence, LabelSenseList>> collector,
			Reporter reporter) throws IOException {
	
		LabelSenseList allSenses = new LabelSenseList() ;
		allSenses.setSenses(new ArrayList<LabelSense>()) ;
		
		for (LabelSenseList senses:senseLists) {
			
			for (LabelSense sense:senses.getSenses()) {
				allSenses.getSenses().add(LabelSense.newBuilder(sense).build()) ;
			}
		}
		
		
		if (isReducer()) {
			
			if (allSenses.getSenses().size() > 1)
				reporter.getCounter(Counts.ambiguous).increment(1L);
			else
				reporter.getCounter(Counts.unambiguous).increment(1L);
			
			Collections.sort(allSenses.getSenses(), new SenseComparator());
			
		}
		
		collector.collect(new Pair<CharSequence, LabelSenseList>(label, allSenses));
	}

	public static class Combiner extends CombinerOrReducer {

		@Override
		public boolean isReducer() {
			return false;
		}

	}

	public static class Reducer extends CombinerOrReducer {

		@Override
		public boolean isReducer() {
			return true;
		}

	}
	
	private static class SenseComparator implements Comparator<LabelSense> {

		@Override
		public int compare(LabelSense a, LabelSense b) {
			
			int cmp = b.getDocCount().compareTo(a.getDocCount()) ;
			
			if (cmp != 0) 
				return cmp ;
			
			cmp = b.getOccCount().compareTo(a.getOccCount()) ;
			
			if (cmp != 0)
				return cmp ;
			
			
			return a.getId().compareTo(b.getId());
			
			
		}
		
	}
	
	
}
