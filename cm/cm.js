
var pb = require('../pb');  // programming base
var db = require('../db');  // database


var  fs, brw; var pg; var pgs;

async function init(dbStr)
{
 var ret = await db.init('pgs::localhost,site,root,root'); fs = ret[0], brw = ret[1]; pg = ret[2]; pgs=ret[3];
 return [fs, brw, pg, pgs];
}

async function tagsByZIndex(tagName = '*')
{
 var ret = [];
 var res = [];
 var tags = await pg.$$eval(tagName.toLowerCase(), (EL) => EL.map( (el) => (el3 = el2 = el1 = el).tagName.toLowerCase() + ' #' + el1.id + ' ' + (' ' + el2.classList).replaceAll(' ', '.') + ' ::' + getComputedStyle(el3).zIndex ));  //el.classList, getComputedStyle(el).zIndex})); 
 tags.forEach
 (
  (tag) => 
  {
   tag = tag.replace('# ', '').replace('. ', '').replace('::auto', '::0').trim();
   var parts = tag.split('::');
   ret.push(Object.fromEntries(new Map([ [parts[1].trim(), parts[0].trim()] ])));
  }
 );
 var [kx, vx] = pb.sortIx(res, false);
 for (var i = 1; i <= tagsByZIndex.length; i++)  ret.push(res[kx[i-1].ix-1]);
 return ret;
}


module.exports = { init, tagsByZIndex, }






