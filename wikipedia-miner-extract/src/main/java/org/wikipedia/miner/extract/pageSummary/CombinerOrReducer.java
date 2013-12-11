package org.wikipedia.miner.extract.pageSummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.struct.LabelCount;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.pageSummary.PageSummaryStep.Unforwarded;


public abstract class CombinerOrReducer extends AvroReducer<PageKey, PageDetail, Pair<PageKey, PageDetail>> {

	private static Logger logger = Logger.getLogger(CombinerOrReducer.class) ;

	public abstract boolean isReducer() ;
	
	private String[] debugTitles = {"Atheist","Atheism","Atheists","Athiest","People by religion"} ;
	
	@Override
	public void reduce(PageKey key, Iterable<PageDetail> pagePartials,
			AvroCollector<Pair<PageKey, PageDetail>> collector,
			Reporter reporter) throws IOException {
		
		
		Integer id = null;
		Integer namespace = key.getNamespace() ;
		String title = key.getTitle().toString() ;
		Long lastEdited = null ;
		
		SortedMap<Integer,PageSummary> redirects = new TreeMap<Integer, PageSummary>() ;
		PageSummary redirectsTo = null ;
		
		SortedMap<Integer,PageSummary> linksIn = new TreeMap<Integer, PageSummary>() ;
		SortedMap<Integer,PageSummary> linksOut = new TreeMap<Integer, PageSummary>() ;
		
		SortedMap<Integer,PageSummary> parentCategories = new TreeMap<Integer, PageSummary>() ;
		SortedMap<Integer,PageSummary> childCategories = new TreeMap<Integer, PageSummary>() ;
		SortedMap<Integer,PageSummary> childArticles = new TreeMap<Integer, PageSummary>() ;
		
		SortedMap<CharSequence,Integer> labelCounts = new TreeMap<CharSequence,Integer>() ;
		
		boolean debug = false ;
		for(String debugTitle:debugTitles) {
			if (title.equalsIgnoreCase(debugTitle))
				debug = true ;
		}
		
		if (debug)
			logger.info("Processing " + key.toString()) ;
		
		for (PageDetail pagePartial: pagePartials) {

			if (debug)
				logger.info("partial: " + pagePartial.toString());
			
			if (pagePartial.getId() != null)
				id = pagePartial.getId() ;
			
			if (pagePartial.getLastEdited() != null)
				lastEdited = pagePartial.getLastEdited() ;
			
			if (pagePartial.getRedirectsTo() != null) {
				
				if (debug)
					logger.info(" -" + pagePartial.getRedirectsTo() + " vs " + redirectsTo) ;
				
				if (redirectsTo == null || redirectsTo.getId() < 0) {
					//always clobber a redirectTo that hasn't been resolved to an id yet
					
					redirectsTo = PageSummary.newBuilder(pagePartial.getRedirectsTo()).build() ;
				}else {
					
					if (pagePartial.getRedirectsTo().getForwarded())
						redirectsTo.setForwarded(true);	
				}
				
				if (debug)
					logger.info(" - " + redirectsTo) ;
			}

			redirects = addToLinkMap(pagePartial.getRedirects(), redirects) ;
			
			linksIn = addToLinkMap(pagePartial.getLinksIn(), linksIn) ;
			linksOut = addToLinkMap(pagePartial.getLinksOut(), linksOut) ;
			
			parentCategories = addToLinkMap(pagePartial.getParentCategories(), parentCategories) ;
			childCategories = addToLinkMap(pagePartial.getChildCategories(), childCategories) ;
			childArticles = addToLinkMap(pagePartial.getChildArticles(), childArticles) ;
			
			for (LabelCount lc:pagePartial.getLabelCounts()) {
				
				//the clone is needed because avro seems to reuse these instances.
				//if we don't clone it, it will get overwritten
				LabelCount lcCopy = LabelCount.newBuilder(lc).build() ;
				
				Integer count = labelCounts.get(lcCopy.getLabel()) ;
				
				if (count == null)
					count = 0 ;
				
				count = count + lcCopy.getCount() ;
				
				labelCounts.put(lcCopy.getLabel(), count) ;
			}
			
		}
		
		if (id == null && isReducer()) {
			
			//if we don't know the id of the page by this point, then it must be the 
			//result of an unresolvable redirect or link (so forget it)
			//logger.warn("Orphaned page title: " + key.getTitle() + " in ns " + key.getNamespace()) ;
			
			return ;
		}
		
					
		if (debug) {
			
			for (Integer rId:redirects.keySet()) 
				logger.info(" - " + rId+ ":" + redirects.get(rId)) ;
			
		}
		
		PageDetail combinedPage = PageSummaryStep.buildEmptyPageDetail() ;
		combinedPage.setId(id)	;
		combinedPage.setTitle(title);
		combinedPage.setNamespace(namespace);
		combinedPage.setLastEdited(lastEdited) ;
		
		combinedPage.setRedirectsTo(redirectsTo);
		
		boolean isRedirect = redirectsTo != null ;
		
		//redirects always need forwarding
		combinedPage.setRedirects(convertToList(redirects, true));
		
		//links in always need to be backtracked (or forwarded by redirects) 
		combinedPage.setLinksIn(convertToList(linksIn, true));
		
		//links out only need to be forwarded by redirects
		combinedPage.setLinksOut(convertToList(linksOut, isRedirect));
		
		//parent categories only need to be forwarded by redirects 
		combinedPage.setParentCategories(convertToList(parentCategories, isRedirect));
		
		//children of both types always need to be backtracked to parent (or forwarded by redirect)
		combinedPage.setChildCategories(convertToList(childCategories, true));
		combinedPage.setChildArticles(convertToList(childArticles, true));
		
		for (Map.Entry<CharSequence,Integer> e:labelCounts.entrySet()) 
			combinedPage.getLabelCounts().add(new LabelCount(e.getKey(), e.getValue())) ;
		
		
		//count stuff that needs to be forwarded, so we know wheither another iteration is needed
		
		if (isReducer()) {
		
			countUnforwarded(Unforwarded.redirect, combinedPage.getRedirects(), reporter) ;
			
			if (redirectsTo != null && redirectsTo.getId() >= 0 && !redirectsTo.getForwarded())
				reporter.incrCounter(Unforwarded.redirect, 1);
			
			countUnforwarded(Unforwarded.linkIn, combinedPage.getLinksIn(), reporter) ;
			countUnforwarded(Unforwarded.linkOut, combinedPage.getLinksOut(), reporter) ;
			
			countUnforwarded(Unforwarded.parentCategory, combinedPage.getParentCategories(), reporter) ;
			countUnforwarded(Unforwarded.childCategory, combinedPage.getChildCategories(), reporter) ;
			countUnforwarded(Unforwarded.childArticle, combinedPage.getChildArticles(), reporter) ;

		}
		
		if (debug)
			logger.info("combined: " + combinedPage.toString());
		
		collector.collect(new Pair<PageKey,PageDetail>(key, combinedPage));
	}
	
	private SortedMap<Integer,PageSummary> addToLinkMap(List<PageSummary> links, SortedMap<Integer,PageSummary> linkMap) {
		
		if (links == null || links.isEmpty())
			return linkMap ;
		
		for (PageSummary link:links) {
		
			//only overwrite if previous entry has not been forwarded
			PageSummary existingLink = linkMap.get(link.getId()) ;
			if (existingLink == null) {
				
				//the clone is needed because avro seems to reuse these instances.
				//if we don't clone it, it will get overwritten later
				linkMap.put(link.getId(), PageSummaryStep.clone(link)) ;
				
			} else {
			
				if (link.getForwarded())
					existingLink.setForwarded(true) ;
				
				//linkMap.put(existingLink.getId(), existingLink) ;
			}
		}
		
		return linkMap ;
	}
	
	private List<PageSummary> convertToList(SortedMap<Integer,PageSummary> linkMap,  boolean requiresForwarding) {
		
		List<PageSummary> links = new ArrayList<PageSummary>() ;
		
		for (PageSummary link:linkMap.values()) {
			if (!requiresForwarding) 
				link.setForwarded(true) ;
		
			links.add(link) ;
		}

		return links ;
	}
	
	private void countUnforwarded(Unforwarded counter, List<PageSummary> links, Reporter reporter) {
		
		for (PageSummary link:links) 				
			if (!link.getForwarded()) 
				reporter.incrCounter(counter, 1);
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