package org.wikipedia.miner.extract.steps.labelOccurrences;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;

public abstract class CombinerOrReducer extends AvroReducer<CharSequence, LabelOccurrences, Pair<CharSequence, LabelOccurrences>> {
	
	public enum Counts {falsePositives, truePositives} ;
	
	public abstract boolean isReducer() ;
	
	@Override
	public void reduce(CharSequence label, Iterable<LabelOccurrences> partials,
			AvroCollector<Pair<CharSequence, LabelOccurrences>> collector,
			Reporter reporter) throws IOException {
	
		LabelOccurrences allOccurrences = new LabelOccurrences(0,0,0,0) ;
		
		for (LabelOccurrences partial:partials) {
			allOccurrences.setLinkDocCount(allOccurrences.getLinkDocCount() + partial.getLinkDocCount()) ;
			allOccurrences.setLinkOccCount(allOccurrences.getLinkOccCount() + partial.getLinkOccCount()) ;
			allOccurrences.setTextDocCount(allOccurrences.getTextDocCount() + partial.getTextDocCount()) ;
			allOccurrences.setTextOccCount(allOccurrences.getTextOccCount() + partial.getTextOccCount()) ;
		}
		
		if (isReducer()) {
			
			if (allOccurrences.getLinkOccCount() == 0) {
				reporter.getCounter(Counts.falsePositives).increment(1L);
				return ; 
			} else {
				reporter.getCounter(Counts.truePositives).increment(1L);
			}
		}

		collector.collect(new Pair<CharSequence, LabelOccurrences>(label, allOccurrences));
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
	
}
