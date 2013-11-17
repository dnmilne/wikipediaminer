package org.wikipedia.miner.web.service;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.wikipedia.miner.model.Wikipedia;
import org.dmilne.xjsf.Service;
import org.dmilne.xjsf.param.StringArrayParameter;

@SuppressWarnings("serial")
public abstract class WMService extends Service {

	public WMService(String groupName, String shortDescription, String detailsMarkup, boolean supportsDirectResponse) {
		super(groupName, shortDescription, detailsMarkup, supportsDirectResponse);
	}

	private WMHub wmHub ;

	protected StringArrayParameter prmWikipedia ;

	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		wmHub = WMHub.getInstance(config.getServletContext()) ;

		String[] valsWikipedia = getWMHub().getWikipediaNames() ;
		String[] dscsWikipedia = new String[valsWikipedia.length] ;

		for (int i=0 ; i<valsWikipedia.length ; i++) {
			dscsWikipedia[i] = getWMHub().getWikipediaDescription(valsWikipedia[i]) ;

			if (dscsWikipedia[i] == null)
				dscsWikipedia[i] = "No description available" ;
		}

		prmWikipedia = new StringArrayParameter("wikipedia", "Which edition of Wikipedia to retrieve information from", getWMHub().getDefaultWikipediaName(), valsWikipedia, dscsWikipedia) ;
		addBaseParameter(prmWikipedia) ;
	}


	public WMHub getWMHub() {
		return wmHub ;
	}	                                                            

	public Wikipedia getWikipedia(HttpServletRequest request) {

		String wikiName = prmWikipedia.getValue(request) ;

		Wikipedia wiki = wmHub.getWikipedia(wikiName) ;

		return wiki ;
	}

	public String getWikipediaName(HttpServletRequest request) {
		return prmWikipedia.getValue(request) ; 
	}

}
