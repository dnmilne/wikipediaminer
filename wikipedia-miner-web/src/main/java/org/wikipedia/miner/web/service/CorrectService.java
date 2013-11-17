package org.wikipedia.miner.web.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.wikipedia.miner.model.Label;
import org.wikipedia.miner.model.Wikipedia;
import org.dmilne.xjsf.UtilityMessages.ParameterMissingMessage;
import org.wikipedia.miner.util.text.TextProcessor;
import org.dmilne.xjsf.Service;
import org.dmilne.xjsf.param.IntParameter;
import org.dmilne.xjsf.param.StringParameter;

import com.google.gson.annotations.Expose;

public class CorrectService extends WMService {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7243235547641000876L;
	
	private StringParameter prmTerm ;
	
	private IntParameter prmMax ;

	public CorrectService() {
		super("query","Provides alternatives for misspelt words",
				"<p></p>", false);
		
		prmTerm = new StringParameter("term", "The term or phrase to find spelling corrections for", null) ;
		addGlobalParameter(prmTerm) ;	
		
		prmMax = new IntParameter("max", "The maximum number of suggestions to return", 10) ;
		addGlobalParameter(prmMax) ;
	}

	public Service.Message buildWrappedResponse(HttpServletRequest request) {
		
		String term = prmTerm.getValue(request) ;
		
		if (term == null) 
			return new ParameterMissingMessage(request) ;
		
		Wikipedia wikipedia = getWikipedia(request) ;
		TextProcessor tp = wikipedia.getEnvironment().getConfiguration().getDefaultTextProcessor() ;
		
		Message msg = new Message(request) ;
		
		int max = prmMax.getValue(request) ;
		
		int count = 0 ;
		for (Suggestion s:getSuggestions(term, wikipedia, tp)) {
			if (count++ > max) break ;
			
			msg.addSuggestion(s) ;
		}
		
		return msg;
	}
	
	
	private TreeSet<Suggestion> getSuggestions(String term, Wikipedia wikipedia, TextProcessor tp) {
		
		TreeSet<Suggestion> suggestions = new TreeSet<Suggestion>() ;
		
		for (String s1:getWordsWithin1Edit(term)) {
			Label l1 = new Label(wikipedia.getEnvironment(), s1, tp) ;
			
			if (l1.exists()) {
				suggestions.add(new Suggestion(s1, 1, l1.getOccCount())) ;
			}
			
			for (String s2:getWordsWithin1Edit(s1)) {
				Label l2 = new Label(wikipedia.getEnvironment(), s2, tp) ;
				
				if (l2.exists()) {
					suggestions.add(new Suggestion(s2, 2, l2.getOccCount())) ;
				}
			}
		}
		
		return suggestions ;
	}
	
	
	
	
	private ArrayList<String> getWordsWithin1Edit(String word) {
		
		ArrayList<String> result = new ArrayList<String>();
		for(int i=0; i < word.length(); ++i) result.add(word.substring(0, i) + word.substring(i+1));
		for(int i=0; i < word.length()-1; ++i) result.add(word.substring(0, i) + word.substring(i+1, i+2) + word.substring(i, i+1) + word.substring(i+2));
		for(int i=0; i < word.length(); ++i) for(char c='a'; c <= 'z'; ++c) result.add(word.substring(0, i) + String.valueOf(c) + word.substring(i+1));
		for(int i=0; i <= word.length(); ++i) for(char c='a'; c <= 'z'; ++c) result.add(word.substring(0, i) + String.valueOf(c) + word.substring(i));
		return result;

	}
	
	public static class Message extends Service.Message {
		
		@Expose
		@ElementList(inline=true, entry="suggestion")
		private TreeSet<Suggestion> suggestions = new TreeSet<Suggestion>() ;
		
		private Message(HttpServletRequest request) {
			super(request) ;
		}
		
		private void addSuggestion(Suggestion s) {
			suggestions.add(s) ;
		}
		
		public SortedSet<Suggestion> getSuggestions() {
			return Collections.unmodifiableSortedSet(suggestions) ;
		}
	}
	
	public static class Suggestion implements Comparable<Suggestion> {

		@Expose
		@Attribute
		private String text ;
		
		@Expose
		@Attribute
		private Integer editDistance ;
		
		@Expose
		@Attribute
		private Long occCount ;
		
		private Suggestion(String text, int editDistance, long occCount) {
			this.text = text ;
			this.editDistance = editDistance ;
			this.occCount = occCount ;
		}

		public int compareTo(Suggestion s) {
			
			int c = editDistance.compareTo(s.editDistance) ;
			if (c != 0)
				return c ;
			
			c = s.occCount.compareTo(occCount) ;
			if (c != 0)
				return c ;
			
			return text.compareTo(s.text) ;
		}

		public String getText() {
			return text;
		}

		public Integer getEditDistance() {
			return editDistance;
		}

		public Long getOccCount() {
			return occCount;
		}
	}

}
