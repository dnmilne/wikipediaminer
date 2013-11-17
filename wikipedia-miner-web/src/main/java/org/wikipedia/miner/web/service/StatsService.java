package org.wikipedia.miner.web.service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.*;
import org.wikipedia.miner.db.WEnvironment.StatisticName;
import org.wikipedia.miner.model.Wikipedia;
import org.dmilne.xjsf.Service;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

@SuppressWarnings("serial")
public class StatsService extends WMService{
	
	DateFormat df ;

	public StatsService() {
		super("meta","Provides statistics of a specific wikipedia version",
				"<p>Retrieves statistics (article counts, last edit date, etc.) for a wikipedia dump.</p>", false
				);
		
		TimeZone tz = TimeZone.getTimeZone("GMT:00");

		df = new SimpleDateFormat("dd MMM yyyy HH:mm:ss z") ;
	    df.setTimeZone( tz );
	}

	@Override
	public Message buildWrappedResponse(HttpServletRequest request) throws Exception {
		
		Message msg = new Message(request, getWikipediaName(request)) ;
		
		Wikipedia w = getWikipedia(request) ;
		
		for (StatisticName statName:StatisticName.values()) {
			
			long stat = w.getEnvironment().retrieveStatistic(statName) ;
			switch(statName) {
			
			case lastEdit: 
				String date = df.format(new Date(stat)) ;
				msg.addStat(statName, date) ;
				break ;
			default: 
				msg.addStat(statName, String.valueOf(stat)) ;
				break ;
			}
		}
		
		return msg;
	}
	
	public static class Message extends Service.Message {
		
		@SerializedName(value="for")
		@Attribute(name="for")
		private String wikiName ;
		
		@Expose
		@ElementMap(inline=true,attribute=true,entry="statistic",key="name")
		private HashMap<StatisticName,String> statistics = new HashMap<StatisticName,String>() ;
		
		private Message(HttpServletRequest request, String wikiName) {
			super(request) ;
			this.wikiName = wikiName ;
		}
		
		private void addStat(StatisticName name, String value) {
			statistics.put(name, value) ;
		}

		public String getWikiName() {
			return wikiName;
		}

		public Map<StatisticName, String> getStatistics() {
			return Collections.unmodifiableMap(statistics);
		}
		
		
	}

}
