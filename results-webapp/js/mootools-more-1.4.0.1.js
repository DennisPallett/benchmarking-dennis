// MooTools: the javascript framework.
// Load this file's selection again by visiting: http://mootools.net/more/3198acdac57c032830e42ff832bc2050 
// Or build this file again with packager using: packager build More/Number.Format
/*
---
copyrights:
  - [MooTools](http://mootools.net)

licenses:
  - [MIT License](http://mootools.net/license.txt)
...
*/
MooTools.More={version:"1.4.0.1",build:"a4244edf2aa97ac8a196fc96082dd35af1abab87"};(function(){var b=function(c){return c!=null;};var a=Object.prototype.hasOwnProperty;
Object.extend({getFromPath:function(e,f){if(typeof f=="string"){f=f.split(".");}for(var d=0,c=f.length;d<c;d++){if(a.call(e,f[d])){e=e[f[d]];}else{return null;
}}return e;},cleanValues:function(c,e){e=e||b;for(var d in c){if(!e(c[d])){delete c[d];}}return c;},erase:function(c,d){if(a.call(c,d)){delete c[d];}return c;
},run:function(d){var c=Array.slice(arguments,1);for(var e in d){if(d[e].apply){d[e].apply(d,c);}}return d;}});})();(function(){var b=null,a={},d={};var c=function(f){if(instanceOf(f,e.Set)){return f;
}else{return a[f];}};var e=this.Locale={define:function(f,j,h,i){var g;if(instanceOf(f,e.Set)){g=f.name;if(g){a[g]=f;}}else{g=f;if(!a[g]){a[g]=new e.Set(g);
}f=a[g];}if(j){f.define(j,h,i);}if(!b){b=f;}return f;},use:function(f){f=c(f);if(f){b=f;this.fireEvent("change",f);}return this;},getCurrent:function(){return b;
},get:function(g,f){return(b)?b.get(g,f):"";},inherit:function(f,g,h){f=c(f);if(f){f.inherit(g,h);}return this;},list:function(){return Object.keys(a);
}};Object.append(e,new Events);e.Set=new Class({sets:{},inherits:{locales:[],sets:{}},initialize:function(f){this.name=f||"";},define:function(i,g,h){var f=this.sets[i];
if(!f){f={};}if(g){if(typeOf(g)=="object"){f=Object.merge(f,g);}else{f[g]=h;}}this.sets[i]=f;return this;},get:function(r,j,q){var p=Object.getFromPath(this.sets,r);
if(p!=null){var m=typeOf(p);if(m=="function"){p=p.apply(null,Array.from(j));}else{if(m=="object"){p=Object.clone(p);}}return p;}var h=r.indexOf("."),o=h<0?r:r.substr(0,h),k=(this.inherits.sets[o]||[]).combine(this.inherits.locales).include("en-US");
if(!q){q=[];}for(var g=0,f=k.length;g<f;g++){if(q.contains(k[g])){continue;}q.include(k[g]);var n=a[k[g]];if(!n){continue;}p=n.get(r,j,q);if(p!=null){return p;
}}return"";},inherit:function(g,h){g=Array.from(g);if(h&&!this.inherits.sets[h]){this.inherits.sets[h]=[];}var f=g.length;while(f--){(h?this.inherits.sets[h]:this.inherits.locales).unshift(g[f]);
}return this;}});})();Locale.define("en-US","Number",{decimal:".",group:",",currency:{prefix:"$ "}});Number.implement({format:function(q){var n=this;q=q?Object.clone(q):{};
var a=function(i){if(q[i]!=null){return q[i];}return Locale.get("Number."+i);};var f=n<0,h=a("decimal"),k=a("precision"),o=a("group"),c=a("decimals");if(f){var e=a("negative")||{};
if(e.prefix==null&&e.suffix==null){e.prefix="-";}["prefix","suffix"].each(function(i){if(e[i]){q[i]=a(i)+e[i];}});n=-n;}var l=a("prefix"),p=a("suffix");
if(c!==""&&c>=0&&c<=20){n=n.toFixed(c);}if(k>=1&&k<=21){n=(+n).toPrecision(k);}n+="";var m;if(a("scientific")===false&&n.indexOf("e")>-1){var j=n.split("e"),b=+j[1];
n=j[0].replace(".","");if(b<0){b=-b-1;m=j[0].indexOf(".");if(m>-1){b-=m-1;}while(b--){n="0"+n;}n="0."+n;}else{m=j[0].lastIndexOf(".");if(m>-1){b-=j[0].length-m-1;
}while(b--){n+="0";}}}if(h!="."){n=n.replace(".",h);}if(o){m=n.lastIndexOf(h);m=(m>-1)?m:n.length;var d=n.substring(m),g=m;while(g--){if((m-g-1)%3==0&&g!=(m-1)){d=o+d;
}d=n.charAt(g)+d;}n=d;}if(l){n=l+n;}if(p){n+=p;}return n;},formatCurrency:function(b){var a=Locale.get("Number.currency")||{};if(a.scientific==null){a.scientific=false;
}a.decimals=b!=null?b:(a.decimals==null?2:a.decimals);return this.format(a);},formatPercentage:function(b){var a=Locale.get("Number.percentage")||{};if(a.suffix==null){a.suffix="%";
}a.decimals=b!=null?b:(a.decimals==null?2:a.decimals);return this.format(a);}});