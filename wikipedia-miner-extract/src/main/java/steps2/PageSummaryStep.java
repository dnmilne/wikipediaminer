package steps2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;

public abstract class PageSummaryStep extends Step{

	public enum PageTypeCount {article, category, disambiguation, redirect, unparseable, rootCategoryId} ; 

	public enum UnforwardedCount {redirect,linkIn,linkOut,parentCategory,childCategory,childArticle} ; 
	
	public PageSummaryStep(Path baseWorkingDir) throws IOException {
		super(baseWorkingDir);
	}

	public long getTotalUnforwarded() {
		
		if (getCounters() == null)
			return 0 ;
		
		long totalUnforwarded = 0 ;
		
		for (UnforwardedCount uc:UnforwardedCount.values()) {
			Counters.Counter counter = getCounters().findCounter(uc) ;
			
			if (counter != null)
				totalUnforwarded = totalUnforwarded + counter.getCounter() ;
		}
	
		return totalUnforwarded ;
		
	}
	
	protected static abstract class CombineOrReduce extends AvroReducer<PageKey, PageDetail, Pair<PageKey, PageDetail>> {

		private static Logger logger = Logger.getLogger(CombineOrReduce.class) ;

		public abstract boolean isReducer() ;
		
		
		@Override
		public void reduce(PageKey key, Iterable<PageDetail> pagePartials,
				AvroCollector<Pair<PageKey, PageDetail>> collector,
				Reporter reporter) throws IOException {
			
			
			Integer id = null;
			Integer namespace = key.getNamespace() ;
			String title = key.getTitle().toString() ;
			
			SortedMap<Integer,PageSummary> redirects = new TreeMap<Integer, PageSummary>() ;
			PageSummary redirectsTo = null ;
			
			SortedMap<Integer,PageSummary> linksIn = new TreeMap<Integer, PageSummary>() ;
			SortedMap<Integer,PageSummary> linksOut = new TreeMap<Integer, PageSummary>() ;
			
			SortedMap<Integer,PageSummary> parentCategories = new TreeMap<Integer, PageSummary>() ;
			SortedMap<Integer,PageSummary> childCategories = new TreeMap<Integer, PageSummary>() ;
			SortedMap<Integer,PageSummary> childArticles = new TreeMap<Integer, PageSummary>() ;
			
			boolean debug = title.equalsIgnoreCase("Percentage") ;

			for (PageDetail pagePartial: pagePartials) {

				if (debug)
					logger.info("partial: " + pagePartial.toString());
				
				if (pagePartial.getId() != null)
					id = pagePartial.getId() ;
				
				if (pagePartial.getRedirectsTo() != null) {
					
					if (redirectsTo == null)
						redirectsTo = pagePartial.getRedirectsTo() ;
					else {
						if (pagePartial.getRedirectsTo().getId() > 0)
							redirectsTo.setId(pagePartial.getRedirectsTo().getId());
						
						if (pagePartial.getRedirectsTo().getForwarded())
							redirectsTo.setForwarded(true);	
					}
				}

				redirects = addToLinkMap(pagePartial.getRedirects(), redirects) ;
				
				linksIn = addToLinkMap(pagePartial.getLinksIn(), linksIn) ;
				linksOut = addToLinkMap(pagePartial.getLinksOut(), linksOut) ;
				
				parentCategories = addToLinkMap(pagePartial.getParentCategories(), parentCategories) ;
				childCategories = addToLinkMap(pagePartial.getChildCategories(), childCategories) ;
				childArticles = addToLinkMap(pagePartial.getChildArticles(), childArticles) ;
				
			}
			
			if (id == null) {
				//if we don't know the id of the page by this point, then it must be the result of an unresolvable redirect or link (so forget it)
				return ;
			}
			
						
			if (debug) {
				
				for (Integer rId:redirects.keySet()) 
					logger.info(" - " + rId+ ":" + redirects.get(rId)) ;
				
			}
			
			PageDetail combinedPage = buildEmptyPageDetail() ;
			combinedPage.setId(id)	;
			combinedPage.setTitle(title);
			combinedPage.setNamespace(namespace);
			
			combinedPage.setRedirectsTo(redirectsTo);
			combinedPage.setRedirects(convertToList(redirects));
			
			combinedPage.setLinksIn(convertToList(linksIn));
			combinedPage.setLinksOut(convertToList(linksOut));
			
			combinedPage.setParentCategories(convertToList(parentCategories));
			combinedPage.setChildCategories(convertToList(childCategories));
			combinedPage.setChildArticles(convertToList(childArticles));
			
			//count stuff that needs to be forwarded along
			
			if (isReducer()) {
			
				countUnforwarded(UnforwardedCount.redirect, combinedPage.getRedirects(), reporter) ;
				if (redirectsTo != null && !redirectsTo.getForwarded())
					reporter.incrCounter(UnforwardedCount.redirect, 1);
				
				countUnforwarded(UnforwardedCount.linkIn, combinedPage.getLinksIn(), reporter) ;
				countUnforwarded(UnforwardedCount.linkOut, combinedPage.getLinksOut(), reporter) ;
				
				countUnforwarded(UnforwardedCount.parentCategory, combinedPage.getParentCategories(), reporter) ;
				countUnforwarded(UnforwardedCount.childCategory, combinedPage.getChildCategories(), reporter) ;
				countUnforwarded(UnforwardedCount.childArticle, combinedPage.getChildArticles(), reporter) ;

			}
			
			if (debug)
				logger.info("combined: " + combinedPage.toString());
			
			collector.collect(new Pair<PageKey,PageDetail>(key, combinedPage));
		}
		
		private SortedMap<Integer,PageSummary> addToLinkMap(List<PageSummary> links, SortedMap<Integer,PageSummary> linkMap) {
			
			if (links == null || links.isEmpty())
				return linkMap ;
			
			for (PageSummary link:links) {
				
				//this is to fix a wierd bug where old links get clobbered
				PageSummary linkCopy = PageSummary.newBuilder(link).build() ;
				
				//only overwrite if previous entry has not been forwarded
				PageSummary existingLink = linkMap.get(linkCopy.getId()) ;
				
				
				if (existingLink == null) {
					linkMap.put(linkCopy.getId(), linkCopy) ;
					continue ;
				}
				
				if (linkCopy.getForwarded())
					existingLink.setForwarded(true) ;
								
				linkMap.put(existingLink.getId(), existingLink) ;
			}
			
			return linkMap ;
		}
		
		private List<PageSummary> convertToList(SortedMap<Integer,PageSummary> linkMap) {
			
			List<PageSummary> links = new ArrayList<PageSummary>() ;
			
			links.addAll(linkMap.values()) ;
			return links ;
		}
		
		private void countUnforwarded(UnforwardedCount counter, List<PageSummary> links, Reporter reporter) {
			
			for (PageSummary link:links) 				
				if (!link.getForwarded()) 
					reporter.incrCounter(counter, 1);
		}
	}
	
	protected static class Combine extends CombineOrReduce {

		@Override
		public boolean isReducer() {
			return false;
		}
		
	}
	
	protected static class Reduce extends CombineOrReduce {

		@Override
		public boolean isReducer() {
			return true;
		}
		
	}
	
	
	static public PageDetail buildEmptyPageDetail() {
		
		PageDetail p = new PageDetail() ;
		
		p.setRedirects(new ArrayList<PageSummary>()) ;
		p.setLinksIn(new ArrayList<PageSummary>());
		p.setLinksOut(new ArrayList<PageSummary>());
		p.setParentCategories(new ArrayList<PageSummary>());
		p.setChildCategories(new ArrayList<PageSummary>());
		p.setChildArticles(new ArrayList<PageSummary>());
		
		return p ;
	}

	
	
}

