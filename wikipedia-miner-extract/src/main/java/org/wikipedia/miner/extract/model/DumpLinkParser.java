package org.wikipedia.miner.extract.model;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.Languages.NamespaceAlias;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.SiteInfo.Namespace;
import org.wikipedia.miner.extract.util.Util;

public class DumpLinkParser {

	private static Logger logger = Logger.getLogger(DumpLinkParser.class) ;
	
	private Language language ;
	private SiteInfo siteInfo ;
	
	
	Pattern langPattern ;
	Pattern namespacePattern ;
	Pattern filePattern ;
	
	public DumpLinkParser(Language lc, SiteInfo si) {
		this.language = lc ;
		this.siteInfo = si ;
		
		langPattern = Pattern.compile("([a-z\\-]+)\\:(.*)", Pattern.DOTALL) ;
		
		List<String> namespaces = new ArrayList<String>() ;

		for (Namespace namespace:siteInfo.getNamespaces())
			namespaces.add(namespace.getName()) ;
		
		for (NamespaceAlias alias:language.getNamespaceAliases())
			namespaces.add(alias.getFrom()) ;
		
		namespacePattern = Pattern.compile("(" + StringUtils.join(namespaces, "|") + ")\\:(.*)", Pattern.CASE_INSENSITIVE + Pattern.DOTALL) ;
		
		//TODO: this should really be loaded from an external file that can be modified easily
		filePattern = Pattern.compile("(.*)\\.(gif|png|jpg|jpeg|ogg|ogv|svg)", Pattern.CASE_INSENSITIVE) ;
		
	}
	
	
	public DumpLink parseLink(String markup, String sourceTitle) throws Exception {
		
		markup = markup.trim();
		
		String lang = null;
		Namespace namespace = null ;
		
		String target = null ; 
		String section = null ;
		String anchor = null ;
		
		//	get language code, if any
		Matcher m = langPattern.matcher(markup) ;
		if (m.matches()) {
			lang = m.group(1) ;
			markup = m.group(2).trim() ;
		}
		
		//get namespace, if any
		m = namespacePattern.matcher(markup) ;
		if (m.matches()) {
			namespace = getNamespace(m.group(1)) ;
			
			markup = m.group(2).trim() ;
		} else {
			namespace = siteInfo.getMainNamespace() ;
		}
		
		
		
		String[] chunks = markup.split("\\|") ;
		
		if (chunks.length == 1) {
			target = chunks[0].trim() ;
			anchor = chunks[0].trim() ;
		} else if (chunks.length == 2) {
			target = chunks[0].trim()  ;
			anchor = chunks[1].trim() ;
		} else {
			target = chunks[0].trim() ;
			
			anchor = chunks[chunks.length-1].trim() ;
		}

		//handle sections
		int poundIndex = target.indexOf('#') ;
		if (poundIndex >= 0) {
			section = target.substring(poundIndex+1) ;		
			target = target.substring(0, poundIndex) ;
		}
		
		//handle files that weren't properly put in the File namespace
		m = filePattern.matcher(target) ;
		if (m.matches()) 
			namespace = siteInfo.getNamespace(SiteInfo.FILE_KEY) ;
		
		//just put up a warning about any links with multiple pipes that were not to files (they are weird)
		if (namespace.getKey() != SiteInfo.FILE_KEY && chunks.length > 2) {
			logger.warn("Too many pipes: " + markup) ;
			
			//TODO: this is hacky, we should have a more graceful way of getting rid of these weird links
			namespace = siteInfo.getNamespace(SiteInfo.SPECIAL_KEY) ;
		}
		
		//handle internal links
		if (target.length() == 0)
			target = sourceTitle ;
		
		//handle pipe trick
		if (anchor.length() == 0)
			anchor = target ;
		
		target = Util.normaliseTitle(target) ;

		return new DumpLink(lang, namespace, target, section, anchor) ;
	}
	
	private Namespace getNamespace(String name) {
		
		
		NamespaceAlias alias = language.getAlias(name) ;
				
		Namespace namespace ;
		if (alias == null)
			namespace = siteInfo.getNamespace(name) ;
		else
			namespace = siteInfo.getNamespace(alias.getTo()) ;
		
		if (namespace == null) {
			logger.warn("Unknown namespace: " + name);
			namespace = siteInfo.getMainNamespace() ;
		}
		
		return namespace ;
	}
	
}
