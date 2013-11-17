package org.wikipedia.miner.web.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.Properties;

public class WebContentRetriever {

	public WebContentRetriever(HubConfiguration config) {
		
		String proxyHost = config.getProxyHost() ;
		String proxyPort = config.getProxyPort() ; 
		
		Properties systemSettings = System.getProperties();
		if (proxyHost != null)
			systemSettings.put("http.proxyHost", proxyHost) ;
		
		if (proxyPort != null)
			systemSettings.put("http.proxyPort", proxyPort) ;

		final String proxyUser = config.getProxyUser() ;
		final String proxyPassword = config.getProxyPassword() ;
		
		if (proxyUser != null && proxyPassword != null) {
			Authenticator.setDefault(new Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(proxyUser, proxyPassword.toCharArray());
				}
			});
		}
	}
	
	public String getWebContent(URL url) throws UnsupportedEncodingException, IOException  {
		
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setInstanceFollowRedirects(true) ;
		
		BufferedReader input = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8")) ;
		String line ;
		
		StringBuffer content = new StringBuffer() ;
		
		while ((line=input.readLine())!=null) {
			
			content.append(line + "\n") ;
		}
			
		return content.toString() ;
	}
}
