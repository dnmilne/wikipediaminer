package org.wikipedia.miner.extract.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.LanguageConfiguration;

public class DumpLinkParser {

	//private LanguageConfiguration languageConfiguration ;
	private SiteInfo siteInfo ;
	
	Pattern langPattern ;
	Pattern namespacePattern ;
	Pattern mainPattern ;
	
	
	public DumpLinkParser(LanguageConfiguration lc, SiteInfo si) {
		//this.languageConfiguration = lc ;
		this.siteInfo = si ;
		
		langPattern = Pattern.compile("([a-z\\-]+)\\:(.*)") ;
		
		StringBuffer tmp = new StringBuffer() ;
		for (String namespace:siteInfo.getNamespaceKeysByNamespace().keySet()) {
			tmp.append(namespace) ;
			tmp.append("|") ;
		}
		tmp.deleteCharAt(tmp.length()-1) ;
		namespacePattern = Pattern.compile("(" + tmp + ")\\:(.*)") ;
		
		
		mainPattern = Pattern.compile("([^#|]+)(\\#([^|]+))?(\\|(.+))?") ;
	}
	
	
	public DumpLink parseLink(String markup) throws Exception {
		
		String lang = null;
		String namespace = null ;
		int namespaceKey = SiteInfo.MAIN_KEY ;
		
		String target = null ; 
		String section = null ;
		String anchor = null ;
		
		//	get language code, if any
		Matcher m = langPattern.matcher(markup) ;
		if (m.matches()) {
			lang = m.group(1) ;
			markup = m.group(2) ;
		}
		
		//get namespace, if any
		m = namespacePattern.matcher(markup) ;
		if (m.matches()) {
			namespace = m.group(1) ;
			namespaceKey = siteInfo.getNamespaceKey(namespace) ;
			markup = m.group(2) ;
		}
		
		m = mainPattern.matcher(markup) ;
		if (m.matches()) {
			target = m.group(1) ;
			
			section = m.group(3) ;
			
			anchor = m.group(5) ;
		} else {
			throw (new Exception("Could not parse link markup: '" + markup + "'")) ;
			
		}
		
		if (anchor == null) 
			anchor = markup ;
		else {
			anchor = anchor.trim();
		}
		
		return new DumpLink(lang, namespaceKey, target, section, anchor) ;
	}
	
}
