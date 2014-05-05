package org.wikipedia.miner.web.util;

import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.Attribute;
import org.dmilne.xjsf.UtilityMessages.ErrorMessage;

import com.google.gson.annotations.Expose;

public class UtilityMessages {
	
	public static class InvalidIdMessage extends ErrorMessage {

		@Expose 
		@Attribute
		private final Integer invalidId ;

		public InvalidIdMessage(HttpServletRequest request, Integer id) {
			super(request, "'" + id + "' is not a valid id") ;	
			invalidId = id ;
		}

		public Integer getInvalidId() {
			return invalidId;
		}
	}

	public static class InvalidTitleMessage extends ErrorMessage {

		@Expose 
		@Attribute
		private final String invalidTitle ;

		public InvalidTitleMessage(HttpServletRequest request, String title) {
			super(request, "'" + title + "' is not a valid title") ;	
			invalidTitle = title ;
		}

		public String getInvalidTitle() {
			return invalidTitle;
		}
		
	}
	
	public static class UnknownTermMessage extends ErrorMessage {
		
		@Expose 
		@Attribute
		private final String unknownTerm ;

		public UnknownTermMessage(HttpServletRequest request, String term) {
			super(request, "'" + term + "' is not a known term") ;	
			unknownTerm = term ;
		}

		public String getUnknownTerm() {
			return unknownTerm;
		}
	}
	
	
}
