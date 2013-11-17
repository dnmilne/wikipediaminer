package org.wikipedia.miner.web.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.*;
import org.dmilne.xjsf.Service;

import com.google.gson.annotations.Expose;

@SuppressWarnings("serial")
public class ListWikipediasService extends WMService{

	public ListWikipediasService() {
		super("meta","Lists available editions of Wikipedia", 
				"<p>This service lists the different editions of Wikipedia that are available</p>",false
				);
	}

	@Override
	public Message buildWrappedResponse(HttpServletRequest request) throws Exception {
		
		Message msg = new Message(request) ;
		
		for (String wikiName: getWMHub().getWikipediaNames()) {
			String desc = getWMHub().getWikipediaDescription(wikiName) ;
			boolean isDefault = wikiName.equals(getWMHub().getDefaultWikipediaName()) ;
			msg.addWikipedia(new Wikipedia(wikiName, desc, isDefault)) ;
		}
		
		return msg ;
	}

	public static class Message extends Service.Message {
		
		@Expose
		@ElementList(inline=true)
		private ArrayList<Wikipedia> wikipedias = new ArrayList<Wikipedia>() ;
		
		private Message(HttpServletRequest request) {
			super(request) ;
		}
		
		private void addWikipedia(Wikipedia w) {
			wikipedias.add(w) ;
		}

		public List<Wikipedia> getWikipedias() {
			return Collections.unmodifiableList(wikipedias);
		}
	}
	
	public static class Wikipedia {
		
		@Expose
		@Attribute
		private String name ;
		
		@Expose
		@Attribute
		private String description ;
		
		@Expose
		@Attribute
		private boolean isDefault ;
		
		private Wikipedia(String name, String description, boolean isDefault) {
			this.name = name ;
			this.description = description ;
			this.isDefault = isDefault ;
		}

		public String getName() {
			return name;
		}

		public String getDescription() {
			return description;
		}

		public boolean isDefault() {
			return isDefault;
		}
	}
}
