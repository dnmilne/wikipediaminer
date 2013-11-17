/**
 * @author   David Simpson david.simpson [at] nottingham.ac.uk
 * @version  1.0 2008-06-18
 * 
 * jGoogleAnalytics - Monitor events with Google Analytics ga.js tracking code
 * 
 * Requires jQuery 1.2.x or higher (for cross-domain $.getScript)
 * 
 * Uses some elements of gaTracker (c)2007 Jason Huck/Core Five Creative
 *    http://plugins.jquery.com/files/jquery.gatracker.js_0.txt  
 *  
 * @param {String} trackerCode
 * @param {Object} options see setings below
 *    
 * usage: 
 *	 $.jGoogleAnalytics( 'UA-XXXXXX-X');
 *	 $.jGoogleAnalytics( 'UA-XXXXXX-X', {anchorClick: true, pageViewsEnabled: false} );
 * 
 */

(function($) { // make available to jQuery only

	$.jGoogleAnalytics = function (trackerCode, options)
	{
		
		settings = $.extend({
			anchorClick:         false,         // adds click tracking to *all* anchors
			clickEvents:         null,          // e.g. {'.popup': '/popup/nasty'}
			crossDomainSelector: false,         // e.g. 'a.crossDomain'
			domainName:          false,         // e.g. 'nottingham.ac.uk'  

			evalClickEvents:     null,          // e.g. {'#menu li a': "'/tabs/'+ $(this).text()"}
			evalSubmitEvents:    null,          // e.g. {'#menu li a': "'/tabs/'+ $(this).text()"}
			
			extensions:          [              
					               'pdf','doc','xls','csv','jpg','gif', 'mp3',
					               'swf','txt','ppt','zip','gz','dmg','xml'		
			                     ],	            // download extensions to track
			
			external:	         '/external/',  // prefix to add to external links
			mailto:		         '/mailto/',    // prefix to add to email addresses
			download:	         '/download/',  // prefix to add to downloads
			
			organicSearch:       null,		    // e.g. {'search-engine': 'query-term', 'google.nottingham.ac.uk': 'q'}
			pageViewsEnabled:    true,          // can be disabled e.g. if only tracking click events
			sampleRate:          null,          // e.g. 50 - set the sample rate at 50%
			submitEvents:        null           // e.g. {'#personUK': '/personsearch/uk'}
		});
		
		if (options)
		{
			$.extend(settings, options);
		} 
		
		init();

		/****** methods *******/
				
		/**
		 * Initialise the tracking code and add any optional functionality
		 */
		function setupTracking()
		{
			// Get the tracking code
			var pageTracker = _gat._getTracker(trackerCode);
				
			// Track visitor across subdomain
			if (settings.topLevelDomain)
			{
				pageTracker._setDomainName(settings.topLevelDomain);
			}
			
			// Set the sample rate - for very busy sites
			if (settings.sampleRate)
			{
				pageTracker._setSampleRate(settings.sampleRate);
			}
			
			// Track visitor across domains		
			if (settings.crossDomainSelector)
			{
				// ignore domain names
				pageTracker._setDomainName('none');
				pageTracker._setAllowLinker(true);
				
				// Add submit event to form selector e.g. form.crossDomain
				$('form' + settings.crossDomainSelector).submit(
					function()
					{
						pageTracker._linkByPost(this);
						// console.debug('crossDomain ._linkByPost');
					}
				);
				// Add a click event to anchor selector e.g. a.crossDomain
				$('a' + settings.crossDomainSelector).click(
					function()
					{
						pageTracker._link( $(this).attr('href') );
						// console.debug('crossDomain ._link: ' + $(this).attr('href'));
					}
				);
				
				// Add click event to link
			}
			
			// Add organic search engines as required
			if (settings.organicSearch)
			{
				$.each(
					settings.organicSearch, 
					function(key, val)
					{
						pageTracker._addOrganic(key, val);
						// console.debug('_addOrganic: ' + key);
					}
				);
			}
			
			// check that this is the correct place
			pageTracker._initData();
			// console.debug('_initData');
			
			addTracking(pageTracker);		
		}
		
		/**
		 * 
		 */
		function addTracking(pageTracker)
		{		
			// 1. Track event triggered 'views'
			
			
			// loop thru each link on the page
			if (settings.anchorClick)
			{
				// From: http://plugins.jquery.com/files/jquery.gatracker.js_0.txt
				$('a').each(function(){
					var u = $(this).attr('href');
					
					if(typeof(u) != 'undefined'){
						var newLink = decorateLink(u);
	
						// if it needs to be tracked manually,
						// bind a click event to call GA with
						// the decorated/prefixed link
						if (newLink.length)
						{
							$(this).click(
								function()
								{
									pageTracker._trackPageview(newLink);
									// console.debug('anchorClick: ' + newLink);
								}
							);
						}
					}				
				});
			}
			
			// loop thru the clickEvents object
			if (settings.clickEvents)
			{
				$.each(settings.clickEvents, function(key, val){
					$(key).click(function(){
						pageTracker._trackPageview(val);
						// console.debug('clickEvents: ' + val);
					})
				});
			}

			// loop thru the evalClickEvents object
			if (settings.evalClickEvents)
			{
				$.each(settings.evalClickEvents, function(key, val){
					$(key).click(function(){
						evalVal = eval(val)
						if (evalVal != '')
						{
							pageTracker._trackPageview(evalVal);
							// console.debug('evalClickEvents: ' + evalVal);
						}
					})
				});			
			}
			
			// loop thru the evalSubmitEvents object
			if (settings.evalSubmitEvents)
			{
				$.each(settings.evalSubmitEvents, function(key, val){
					$(key).submit(function(){
						evalVal = eval(val)
						if (evalVal != '')
						{
							pageTracker._trackPageview(evalVal);
							// console.debug('evalSubmitEvents: ' + evalVal);
						}						
					})
				});
			}
			
			// loop thru the submitEvents object
			if (settings.submitEvents)
			{
				$.each(settings.submitEvents, function(key, val){
					$(key).submit(function(){
						pageTracker._trackPageview(val);
						// console.debug('submitEvents: ' + val);
					})
				});
			}

			// 2. Track normal page views
			if (settings.pageViewsEnabled)
			{
				pageTracker._trackPageview();	
				// console.debug('pageViewsEnabled');
			}
			else
			{
				// console.debug('pageViewsDisabled');		
			}
		}

		// From: http://plugins.jquery.com/files/jquery.gatracker.js_0.txt
		// Returns the given URL prefixed if it is:
		//		a) a link to an external site
		//		b) a mailto link
		//		c) a downloadable file
		// ...otherwise returns an empty string.
		function decorateLink(u)
		{
			var trackingURL = '';
			
			if (u.indexOf('://') == -1 && u.indexOf('mailto:') != 0)
			{
				// no protocol or mailto - internal link - check extension
				var ext = u.split('.')[u.split('.').length - 1];			
				var exts = settings.extensions;
				
				for(i=0; i < exts.length; i++)
				{
					if(ext == exts[i])
					{
						trackingURL = settings.download + u;
						break;
					}
				}				
			} 
			else 
			{
				if (u.indexOf('mailto:') == 0)
				{
					// mailto link - decorate
					trackingURL = settings.mailto + u.substring(7);					
				} 
				else 
				{
					// complete URL - check domain
					var regex = /([^:\/]+)*(?::\/\/)*([^:\/]+)(:[0-9]+)*\/?/i;
					var linkparts = regex.exec(u);
					var urlparts = regex.exec(location.href);
										
					if (linkparts[2] != urlparts[2])
					{
						trackingURL = settings.external + u;
					}
				}
			}
			
			return trackingURL;			
		}
		
		/**
		 * load ga.js and add the tracking code
		 */		
		function init()
		{
			try
			{
				// determine whether to include the normal or SSL version
				var gaUrl = (location.href.indexOf('https') == 0 ? 'https://ssl' : 'http://www');
				gaUrl += '.google-analytics.com/ga.js';
		
				// include ga.js 
				$.getScript(gaUrl, 
					function()
					{
						// console.debug('ga.js loaded');
						setupTracking();						
					}
				);
			} 
			catch(err) 
			{
				// log any failure
				// console.log('Failed to load Google Analytics:' + err);
			}			
		}	
	} 
})(jQuery);