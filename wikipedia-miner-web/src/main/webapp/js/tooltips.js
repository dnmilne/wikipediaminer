var wm_host = "" ;

$.fn.qtip.defaults.style.classes = "ui-tooltip-wm ui-tooltip-rounded ui-tooltip-shadow" ;

/*
$.fn.qtip.styles.wm_definition = { // Last part is the name of the style
   color: 'white',
   background: '#373737' ,
   fontSize: '14px',
   border: {
   	 width: 1,
     radius: 10,
	 color: '#373737'
   },
   position: {
   	 corner: {
   	 	target: 'bottomMiddle',
   	 	tooltip: 'topMiddle'
   	 }
   },
   tip: 'topMiddle', 
   padding: 3 , 
   textAlign: 'left',
   width:400, 
   name: 'cream'
}

$.fn.qtip.styles.wmstyle = { // Last part is the name of the style
   color: 'white',
   background: '#373737' ,
   fontSize: '14px',
   border: {
   	 width: 5,
     radius: 10,
	 color: '#373737'
   },
   padding: 3 , 
   textAlign: 'left',
   tip: true, 
   name: 'cream'
}


*/



function wm_setHost(hostName) {
	wm_host = hostName ;
}

function wm_addDefinitionTooltipsToAllLinks(container, className) {
        
    if (container == null) 
	container = $("body") ;
    
    var links ;

	if (className == null)
		links = container.find("a") ;
	else
		links = container.find("a." + className) ;
        
    $.each(links, function() {
            
        var link = $(this);
        var details = {id:link.attr("pageId"),linkProb:link.attr("linkProb"),relatedness:link.attr("relatedness")} ;
        
        if (details.id != null) {
        	link.qtip(
				{
					content: {
						text: "<div class='ui-tooltip-loading'></div>",
						ajax: {
							url: wm_host + "/services/exploreArticle", // URL to the local file
							type: 'GET', // POST or GET
							data: {
								id: details.id,
								responseFormat: 'JSON',
								definition: true,
								linkFormat: 'PLAIN',
								images: true,
								maxImageWidth: "100",
								maxImageHeight: "100"
							}, // Data to pass along with your request
							success: function(data, status){
							
								// Process the data
								
								var newContent = $("<div/>");
								
								var images = data.images;
								if (images != undefined) 
									newContent.append("<img class='ui-tooltip-wm-icon' src='" + images[0].url + "'></img>");
								
								var definition = data.definition;
								
								if (definition != undefined) 
									newContent.append(definition);
								else 
									newContent.append("no definition available");
								
								if (details.linkProb != null) 
									newContent.append("<p class='ui-tooltip-wm-extraInfo'><b>" + Math.round(details.linkProb * 100) + "%</b> probability of being a link");
								
								if (details.relatedness != null) 
									newContent.append("<p class='ui-tooltip-wm-extraInfo'><b>" + Math.round(details.relatedness * 100) + "%</b> related");
								
								// Set the content manually (required!)
								this.set('content.text', newContent);
							}
						}
			    	},
					style: {
						classes: 'ui-tooltip-rounded ui-tooltip-shadow ui-tooltip-wm-definition',
						width: 500,
					},
					position: {
					   	my: 'top center',
					   	at: 'bottom center'
				    },
					   
				}
			) ;
        }  
     
    }) ;       
}