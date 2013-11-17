//default tooltip style


//gathering url params

var urlParams = {};

(function () {
    var e,
        a = /\+/g,  // Regex for replacing addition symbol with a space
        r = /([^&=]+)=?([^&]*)/g,
        d = function (s) { return decodeURIComponent(s.replace(a, " ")); },
        q = window.location.search.substring(1);

    while (e = r.exec(q))
       urlParams[d(e[1])] = d(e[2]);
})();


function normalize(val, min, max) {
	
	if (min == max) return 1 ;
	
	return (val-min) * (1/(max-min)) ;
}

