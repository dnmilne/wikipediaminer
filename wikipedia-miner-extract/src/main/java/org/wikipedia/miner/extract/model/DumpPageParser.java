package org.wikipedia.miner.extract.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.* ;
import java.io.*;

import javax.xml.stream.*;

import org.apache.log4j.*;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.Languages.NamespaceAlias;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.SiteInfo.Namespace;
import org.wikipedia.miner.model.Page.PageType ;

/**
 * @author David Milne
 *
 * Parses the markup of a &gt;page&lt; element from a mediawiki dump, to convert it into a DumpPage object.
 */
public class DumpPageParser {
	
	private XMLInputFactory xmlStreamFactory = XMLInputFactory.newInstance() ;

	private enum DumpTag {page, id, title, text, timestamp, ignorable} ;
	
	private Language language ;
	private SiteInfo siteInfo ;
	
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") ;
	

	public DumpPageParser(Language lc, SiteInfo si) {
		this.language = lc ;
		this.siteInfo = si ;

	}

	public DumpPage parsePage(String markup) throws XMLStreamException {

		Integer id = null ;
		String title = null ;
		String text = null ;
		Date lastEdited = null ;
		StringBuffer characters = new StringBuffer() ;

		XMLStreamReader xmlStreamReader = xmlStreamFactory.createXMLStreamReader(new StringReader(markup)) ;

		while (xmlStreamReader.hasNext()) {

			int eventCode = xmlStreamReader.next();

			switch (eventCode) {
			case XMLStreamReader.START_ELEMENT :
				break;
			case XMLStreamReader.END_ELEMENT :

				switch(resolveDumpTag(xmlStreamReader.getLocalName())) {

				case id:
					//only take the first id (there is a 2nd one for the revision) 
					if (id == null) 
						id = Integer.parseInt(characters.toString().trim()) ;
					break ;
				case title:
					title = characters.toString().trim() ;
					break ;
				case text:
					text = characters.toString().trim() ;
					break ;
				case timestamp:
					try {
						lastEdited = dateFormat.parse(characters.toString().trim()) ;
					} catch (ParseException e) {
						lastEdited = null ;
					}
					break ;
				}

				characters = new StringBuffer() ;

				break;
			case XMLStreamReader.CHARACTERS :
				characters.append(xmlStreamReader.getText()) ;
			}
		}
		xmlStreamReader.close();

		if (id == null || title == null || text == null) 
			throw new XMLStreamException("Could not parse xml markup for page") ;
		
		
		//identify namespace - assume 0 (main) if there is no prefix, or if prefix doesn't match any known namespaces
		Namespace namespace ;
		int pos = title.indexOf(":") ;
		if (pos > 0) {
			namespace = getNamespace(title.substring(0, pos)) ;
			
			if (namespace == null) 
				namespace = siteInfo.getMainNamespace() ;
			else 
				title = title.substring(pos+1) ;	
		} else {
			namespace = siteInfo.getMainNamespace() ;
		}
		
		
		//ignore anything that isn't in main, category or template namespace
		if (namespace.getKey() != SiteInfo.CATEGORY_KEY && namespace.getKey() != SiteInfo.MAIN_KEY && namespace.getKey() != SiteInfo.TEMPLATE_KEY) {
			Logger.getLogger(DumpPageParser.class).info("Ignoring page " + id + ":" + title) ;
			return null ;
		}
		
		//identify page type ;
		PageType type ;
		String redirectTarget = null ;
		
		
		Matcher redirectMatcher = language.getRedirectPattern().matcher(text) ;
		if (redirectMatcher.find()) {
			
			type = PageType.redirect ;
			
			if (redirectMatcher.group(2) != null)
				redirectTarget = redirectMatcher.group(2) ;
			else
				redirectTarget = redirectMatcher.group(3) ;
			
		} else if (namespace.getKey() == SiteInfo.CATEGORY_KEY) {
			type = PageType.category ;
		} else if (namespace.getKey() == SiteInfo.TEMPLATE_KEY) {
			type = PageType.template ;
		} else if (namespace.getKey() == SiteInfo.MAIN_KEY){
			
			Matcher disambigMatcher = language.getDisambigPattern().matcher(text) ;
			if (disambigMatcher.find()) {
				type = PageType.disambiguation ;
			} else {
				type = PageType.article ;
			}
		} else {
			type = PageType.invalid ;
		}
		
		title = Util.normaliseTitle(title) ;
		
		return new DumpPage(id, namespace, type, title, text, redirectTarget, lastEdited) ;
		
	}


	private DumpTag resolveDumpTag(String tagName) {

		try {
			return DumpTag.valueOf(tagName) ;
		} catch (IllegalArgumentException e) {
			return DumpTag.ignorable ;
		}
	}

private Namespace getNamespace(String name) {
		
		NamespaceAlias alias = language.getAlias(name) ;
		
		if (alias == null)
			return siteInfo.getNamespace(name) ;
		else
			return siteInfo.getNamespace(alias.getTo()) ;
	
	}
	
}
