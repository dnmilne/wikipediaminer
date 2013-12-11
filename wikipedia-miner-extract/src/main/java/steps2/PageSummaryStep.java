package steps2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.struct.LabelCount;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;




public abstract class PageSummaryStep {

	/*
	//
	//public enum Unforwarded {redirect,linkIn,linkOut,parentCategory,childCategory,childArticle} ; 

	
	public PageSummaryStep(Path baseWorkingDir) throws IOException {
		super(baseWorkingDir);
	}

	public long getTotalUnforwarded() {
		
		if (getCounters() == null)
			return 0 ;
		
		long totalUnforwarded = 0 ;
		
		for (Unforwarded uc:Unforwarded.values()) {
			Counters.Counter counter = getCounters().findCounter(uc) ;
			
			if (counter != null)
				totalUnforwarded = totalUnforwarded + counter.getCounter() ;
		}
	
		return totalUnforwarded ;
		
	}
	
	
	
	
	
	
	
	
	*/
	
	
	
	

	
	
}

