var colors = new Array() ;
colors.push({r:5,g:10,b:255}) ;
colors.push({r:230,g:80,b:80}) ;
colors.push({r:255,g:255,b:0}) ;
colors.push({r:50,g:195,b:50}) ;

function getColor(val) {
	if (val==1) 
		val = 0.999999999;
	
	var region = Math.floor(val*3) ;
	var regionWeight = val*3 - region ;
	
	var color1 = colors[region] ;
	var color2 = colors[region+1] ;
	
	var r = color1.r + Math.round(regionWeight * (color2.r - color1.r)) ;
	var g = color1.g + Math.round(regionWeight * (color2.g - color1.g)) ;
	var b = color1.b + Math.round(regionWeight * (color2.b - color1.b)) ;
	
	
	return "rgb(" + r + "," + g + "," + b + ")" ;
	
}