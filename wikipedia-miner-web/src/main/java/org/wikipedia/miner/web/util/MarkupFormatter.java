package org.wikipedia.miner.web.util;

import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.wikipedia.miner.model.Article;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.EmphasisResolver;
import org.wikipedia.miner.util.MarkupStripper;
import org.dmilne.xjsf.param.EnumParameter;

import com.sleepycat.je.DatabaseException;

public class MarkupFormatter {
	
	public enum EmphasisFormat{PLAIN,WIKI,HTML} ;
	public enum LinkFormat{PLAIN,HTML,WIKI,WIKI_ID} ;
	
	private EmphasisResolver emphasisResolver = new EmphasisResolver() ;
	private MarkupStripper stripper = new MarkupStripper() ;
	
	
	private EnumParameter<EmphasisFormat> prmEmphasisFormat ;
	private EnumParameter<LinkFormat> prmLinkFormat ;
	
	
	protected Pattern linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.DOTALL) ;
	
	
	public MarkupFormatter() {
		String[] descEmphasisFormat = {"all emphasis discarded","as mediawiki markup", "as html markup"} ;
		prmEmphasisFormat = new EnumParameter<EmphasisFormat>("emphasisFormat", "The format of bold and italic markup within returned snippets", EmphasisFormat.HTML, EmphasisFormat.values(), descEmphasisFormat) ;

		String[] descLinkFormat = {"all links discarded", "as html links to wikipedia", "as mediawiki markup", "as modified mediawiki markup [[id|anchor]]"} ;
		prmLinkFormat = new EnumParameter<LinkFormat>("linkFormat", "The format of link markup within returned snippets", LinkFormat.HTML, LinkFormat.values(), descLinkFormat) ;
	}
	
	public EnumParameter<EmphasisFormat> getEmphasisFormatParam() {
		return prmEmphasisFormat ;
	}
	
	public EnumParameter<LinkFormat> getLinkFormatParam() {
		return prmLinkFormat ;
	}

	/**
	 * Returns a copy of the given snippet of media-wiki markup, where links to the given topics 
	 * have been replaced with bold emphasis, and all other links have been discarded. 
	 * 
	 * @param markup the mediawiki markup to be modified
	 * @param topicIds a HashSet of topic ids that are to be highlighted
	 * @param wikipedia an instance of wikipedia that can be used to resolve links
	 * @return the modified snippet of mediawiki markup
	 */
	public String highlightTopics(String markup, HashSet<Integer> topicIds, Wikipedia wikipedia) {
				
		Matcher m = linkPattern.matcher(markup) ;

		int lastPos = 0 ;
		StringBuffer sb = new StringBuffer() ;

		while(m.find()) {

			String link = m.group(1) ;
			String anchor ;
			String dest ;

			int pos = link.lastIndexOf("|") ;

			if (pos >1) {
				dest = link.substring(0,pos) ;
				anchor = link.substring(pos+1) ;
			} else {
				dest = link ;
				anchor = link ;
			}

			Article art = wikipedia.getArticleByTitle(dest) ;
			
			sb.append(markup.substring(lastPos, m.start())) ;

			if (art != null && topicIds.contains(art.getId())) {
				sb.append("'''") ;
				sb.append(anchor) ;
				sb.append("'''") ;
			} else {
				sb.append(anchor) ;
			}
			lastPos = m.end() ;	
		}

		sb.append(markup.substring(lastPos)) ;
		return sb.toString() ;
	}
	
	/**
	 * 
	 * 
	 * @param markup
	 * @param emphasisFormat
	 * @param linkFormat
	 * @param wikipedia
	 * @return
	 * @throws DatabaseException
	 */
	public String format(String markup, HttpServletRequest request, Wikipedia wikipedia) throws DatabaseException {
		
		markup = stripper.stripAllButInternalLinksAndEmphasis(markup, null) ;
		
		//deal with emphasis formatting
		
		EmphasisFormat emphasisFormat = prmEmphasisFormat.getDefaultValue() ;
		if (request != null)
			emphasisFormat = prmEmphasisFormat.getValue(request) ;
		
		
		switch(emphasisFormat) {
		
		case PLAIN :
			markup = stripper.stripEmphasis(markup, null) ;
		case HTML :
			markup = emphasisResolver.resolveEmphasis(markup) ;
			break ;
		}
		
		// deal with links
		
		LinkFormat linkFormat = prmLinkFormat.getDefaultValue() ;
		if (request != null)
			linkFormat = prmLinkFormat.getValue(request) ;
		
		if (linkFormat == LinkFormat.WIKI) 
			return markup ;
		
		if (linkFormat == LinkFormat.PLAIN) {
			markup = stripper.stripInternalLinks(markup, null) ;
			return markup ;
		}
					
		Matcher m = linkPattern.matcher(markup) ;
			
		int lastPos = 0 ;
		StringBuffer sb = new StringBuffer() ;
			
		while(m.find()) {
			sb.append(markup.substring(lastPos, m.start())) ;
				
			String link = m.group(1) ;
			String anchor ;
			String dest ;

			int pos = link.lastIndexOf("|") ;

			if (pos >1) {
				dest = link.substring(0,pos) ;
				anchor = link.substring(pos+1) ;
			} else {
				dest = link ;
				anchor = link ;
			}

			Article art = wikipedia.getArticleByTitle(dest) ;
				
			if (art == null) {
				sb.append(anchor) ;
			} else {
				switch(linkFormat) {

				case HTML:
					sb.append("<a href=\"http://www." + wikipedia.getConfig().getLangCode() + ".wikipedia.org/wiki/" + art.getTitle() + "\">") ;
					sb.append(anchor) ;
					sb.append("</a>") ;
					break ;
				case WIKI_ID:
					sb.append("[[" + art.getId() + "|" + anchor + "]]") ; 
					break ;
				}
			}
			lastPos = m.end() ;		
		}
		
		sb.append(markup.substring(lastPos)) ;
		markup = sb.toString() ;
		
		if (linkFormat != LinkFormat.WIKI && linkFormat != LinkFormat.WIKI_ID) {
			markup = markup.replaceAll("\\[\\[", "") ;
			markup = markup.replaceAll("\\]\\]", "") ;
		}
		
		return markup ;
	}
	
	
}
