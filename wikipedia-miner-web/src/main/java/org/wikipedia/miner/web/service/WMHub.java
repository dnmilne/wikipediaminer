package org.wikipedia.miner.web.service;

import java.io.File;
import java.util.HashMap;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.wikipedia.miner.comparison.ArticleComparer;
import org.wikipedia.miner.comparison.ConnectionSnippetWeighter;
import org.wikipedia.miner.comparison.LabelComparer;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.WikipediaConfiguration;
import org.wikipedia.miner.web.util.HubConfiguration;
import org.wikipedia.miner.web.util.MarkupFormatter;
import org.wikipedia.miner.web.util.WebContentRetriever;

public class WMHub {
	
	private static WMHub instance ;
	
	private HubConfiguration config ;
	private HashMap<String, Wikipedia> wikipediasByName ;
	
	private HashMap<String, ArticleComparer> articleComparersByWikiName ;
	private HashMap<String, LabelComparer> labelComparersByWikiName ;
	private HashMap<String, ConnectionSnippetWeighter> snippetWeightersByWikiName ;
		
	private MarkupFormatter formatter = new MarkupFormatter() ;
	private WebContentRetriever retriever ;
		
	// Protect the constructor, so no other class can call it
	private WMHub(ServletContext context) throws ServletException {

		wikipediasByName = new HashMap<String, Wikipedia>() ;
		articleComparersByWikiName = new HashMap<String, ArticleComparer>()  ;
		labelComparersByWikiName = new HashMap<String, LabelComparer>()  ;
		snippetWeightersByWikiName = new HashMap<String, ConnectionSnippetWeighter>() ;
				
		try {
			String hubConfigFile = context.getInitParameter("hubConfigFile") ;
			config = new HubConfiguration(new File(hubConfigFile)) ; 
			
			for (String wikiName:config.getWikipediaNames()) {
				File wikiConfigFile = new File(config.getWikipediaConfig(wikiName)) ;
				WikipediaConfiguration wikiConfig = new WikipediaConfiguration(wikiConfigFile);
				
				
				
				Wikipedia wikipedia = new Wikipedia(wikiConfig, true) ;
				wikipediasByName.put(wikiName, wikipedia) ;
				
				ArticleComparer artCmp = new ArticleComparer(wikipedia) ;
				articleComparersByWikiName.put(wikiName, artCmp) ;
				
				if (artCmp != null && wikiConfig.getLabelDisambiguationModel() != null && wikiConfig.getLabelComparisonModel() != null) {
					LabelComparer lblCmp = new LabelComparer(wikipedia, artCmp) ;
					labelComparersByWikiName.put(wikiName, lblCmp) ;
				}
				
				ConnectionSnippetWeighter sw = new ConnectionSnippetWeighter(wikipedia, artCmp) ;
				snippetWeightersByWikiName.put(wikiName, sw) ;
			}
		
			retriever = new WebContentRetriever(config) ;
			
		} catch (Exception e) {
			throw new ServletException(e) ;
		}
	} 
	  
	public static WMHub getInstance(ServletContext context) throws ServletException {
		
		if (instance != null) 
			return instance ;
		
		instance = new WMHub(context) ;
		return instance ;
		
	}
	
	public String getDefaultWikipediaName() {
		return config.getDefaultWikipediaName() ;
	}
	
	public Wikipedia getWikipedia(String wikiName) {
		return wikipediasByName.get(wikiName) ;
	}
	
	public String getWikipediaDescription(String wikiName) {
		return config.getWikipediaDescription(wikiName) ;
	}
	
	public String[] getWikipediaNames() {
		
		Set<String> wikipediaNames = wikipediasByName.keySet() ;
		return wikipediaNames.toArray(new String[wikipediaNames.size()]) ;
	}
	
	public ArticleComparer getArticleComparer(String wikiName) {
		return articleComparersByWikiName.get(wikiName) ;
	}
	
	public LabelComparer getLabelComparer(String wikiName) {
		return labelComparersByWikiName.get(wikiName) ;
	}
	
	public ConnectionSnippetWeighter getConnectionSnippetWeighter(String wikiName) {
		return snippetWeightersByWikiName.get(wikiName) ;
	}
	
	public MarkupFormatter getFormatter() {
		return formatter ;
	}
	
	public WebContentRetriever getRetriever() {
		return retriever ;
	}
}
