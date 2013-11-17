package org.wikipedia.miner.web.service;

import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.Attribute;
import org.wikipedia.miner.model.Wikipedia;
import org.dmilne.xjsf.Service;

import com.google.gson.annotations.Expose;

@SuppressWarnings("serial")
public class ProgressService extends WMService {
	
	public ProgressService() {
		super("meta","Monitors progress of service initialization",
				"<p>Wikipedia Miner can take a while to get started. This service allows polling to see how much progress has been made loading up a particular edition of Wikipedia</p>", false
		);
	}
	
	
	public Message buildWrappedResponse(HttpServletRequest request) {
		
		Wikipedia wikipedia = getWikipedia(request) ;
		
		double progress = wikipedia.getEnvironment().getProgress() ;
		
		return new Message(request, progress) ;
	}
		
	@Override
	public int getUsageCost(HttpServletRequest request) {
		return 0 ;
	}
	
	public static class Message extends Service.Message {
		
		@Expose
		@Attribute
		private double progress ;
		
		private Message(HttpServletRequest request, double progress) {
			super(request) ;
			this.progress = progress ;
		}

		public double getProgress() {
			return progress;
		}
	}

}
