package org.wikipedia.miner.web.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

public class WebContentRetriever {
    private HubConfiguration hubConf;
    
	public WebContentRetriever(HubConfiguration config) {
		
		String proxyHost = config.getProxyHost() ;
		String proxyPort = config.getProxyPort() ; 
		hubConf=config;
		Properties systemSettings = System.getProperties();
		if (proxyHost != null)
			systemSettings.put("http.proxyHost", proxyHost) ;
		
		if (proxyPort != null)
			systemSettings.put("http.proxyPort", proxyPort) ;

		final String proxyUser = config.getProxyUser() ;
		final String proxyPassword = config.getProxyPassword() ;
		
		if (proxyUser != null && proxyPassword != null) {
			Authenticator.setDefault(new Authenticator() {
                                @Override
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(proxyUser, proxyPassword.toCharArray());
				}
			});
		}
	}
	
	public String getWebContent(URL url) throws UnsupportedEncodingException, IOException  {
            
		URLConnection connection = url.openConnection();
                connection.setRequestProperty("Accept-Charset", "UTF-8");
                InputStream response = connection.getInputStream();
                //int status = ((HttpURLConnection)connection).getResponseCode();

		BufferedReader input = new BufferedReader(new InputStreamReader(response, "UTF-8")) ;
		String line ;
		
		StringBuilder content = new StringBuilder() ;
		
		while ((line=input.readLine())!=null) {
			
			content.append(line).append("\n") ;
		}
			
		return content.toString() ;
	}

    /**
     * @return the hubConf
     */
    public HubConfiguration getHubConf() {
        return hubConf;
    }

    /**
     * @param hubConf the hubConf to set
     */
    public void setHubConf(HubConfiguration hubConf) {
        this.hubConf = hubConf;
    }
}
