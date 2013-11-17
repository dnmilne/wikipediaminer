//for tag clouds

var minSize = 14 ;
var maxSize = 20 ;

var minColor = {r:214, g:125, b:9} ;
var maxColor = {r:226, g:170, b:11} ;

function getFontSize(weight) {
	return minSize + ((maxSize-minSize) * weight) ;
}

function getFontColor(weight) {

	var r = minColor.r + Math.round(weight * (maxColor.r - minColor.r)) ;
	var g = minColor.g + Math.round(weight * (maxColor.g - minColor.g)) ;
	var b = minColor.b + Math.round(weight * (maxColor.b - minColor.b)) ;
	return "rgb(" + r + "," + g + "," + b + ")" ;
}