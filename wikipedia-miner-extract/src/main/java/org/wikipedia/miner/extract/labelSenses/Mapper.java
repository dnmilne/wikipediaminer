package org.wikipedia.miner.extract.labelSenses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.struct.LabelSense;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.pageDepth.InitialDepthMapper;
import org.wikipedia.miner.extract.pageDepth.SubsequentDepthMapper;
import org.wikipedia.miner.extract.util.SiteInfo;

public class Mapper extends AvroMapper<Pair<PageKey, PageDetail>, Pair<CharSequence, LabelSenseList>> {

	private static Logger logger = Logger.getLogger(SubsequentDepthMapper.class) ;
	
	@Override
	public void map(Pair<PageKey, PageDetail> pair,
			AvroCollector<Pair<CharSequence, LabelSenseList>> collector,
			Reporter reporter) throws IOException {
		
	
		PageKey pageKey = pair.key() ;
		PageDetail page = pair.value() ;
		
		//we only care about articles
		if (!pageKey.getNamespace().equals(SiteInfo.MAIN_KEY))
			return ;
		
		//we don't care about redirects
		if (page.getRedirectsTo() != null)
			return ;

		Map<CharSequence,LabelSense> labelSenses = new HashMap<CharSequence,LabelSense>() ;
		
		for (Entry<CharSequence, LabelSummary> e:page.getLabels().entrySet()) {
			
			CharSequence label = e.getKey() ;
			LabelSummary stats = e.getValue() ;
			
			LabelSense sense = new LabelSense() ;
			
			sense.setId(page.getId()) ;
			sense.setDocCount(stats.getDocCount());
			sense.setOccCount(stats.getDocCount());
			sense.setFromTitle(false);
			sense.setFromRedirect(false);
			
			labelSenses.put(label, sense) ;
		}
		
		
		LabelSense titleSense = labelSenses.get(page.getTitle()) ;
		if (titleSense == null) {
			
			titleSense = new LabelSense() ;
			
			titleSense.setId(page.getId()) ;
			titleSense.setDocCount(0);
			titleSense.setOccCount(0);

			titleSense.setFromRedirect(false);	
		} 
		titleSense.setFromTitle(true);
		labelSenses.put(page.getTitle(), titleSense) ;
		
		
		for (PageSummary redirect : page.getRedirects()) {
			
			LabelSense redirectSense = labelSenses.get(redirect.getTitle()) ;
			
			if (redirectSense == null) {
				redirectSense = new LabelSense() ;
				
				redirectSense.setId(page.getId()) ;
				redirectSense.setDocCount(0);
				redirectSense.setOccCount(0);
				redirectSense.setFromTitle(false);
			} 
			redirectSense.setFromRedirect(true);
			labelSenses.put(redirect.getTitle(), redirectSense) ;
		}
		
		for (Entry<CharSequence, LabelSense> e:labelSenses.entrySet()) {
			
			LabelSenseList s = new LabelSenseList() ;
			s.setSenses(new ArrayList<LabelSense>()) ;
			s.getSenses().add(e.getValue()) ;
			
			collector.collect(new Pair<CharSequence,LabelSenseList>(e.getKey(), s));
		}
		
	}
	
}