

var fs     = require('fs'); //Filesystem
var lzw    = require('lzutf8'); //compress-decompress
var ppt    = require('puppeteer-core');  var brw; var pg;
var pgLib  = require('pg').Client; //postgres 
var pgs    = new pgLib({host:"localhost", port: 5432, user:"root", password:"root", database: "site"});
var cpc    = require('child_process'); //spawn & execFile

/*
import('../bm/bm.js').then
(
 t => 
 {
  //console.log(util.inspect(t));
  //t.default();
 }
)

.catch(err => {
    console.error(err);
});
*/

var      delay = ms => new Promise( res => setTimeout(res, ms));

function rUp(a) { if (a.length == 0) return null; a.push(a[0]); a.shift(); return a[a.length-1]; }
function rDn(a) { if (a.length == 0) return null; for (var i = 1; i < a.length; i++) rUp(a); return a[0]; }

function errLog() {return console.log('err');}
function finish() { try {pgs.end();} catch(err){} try {brw.close();} catch(err){} }
function sortIx(sMap, asc=true) // sMap = [ {'a':'A'}, {'b':'X'}, {'c':'M'} ... ]
{
 var kx =[]; var vx =[]; //KeyIndex, ValIndex
 for (var ix = 1; ix <= sMap.length; ix++) Object.entries(sMap[ix-1]).forEach( ([key, val]) => { kx.push({key, ix}); vx.push({val, ix}) });
 if (asc) { kx.sort( (a,b) => { return (a.key < b.key) ?  -1 :  1} ); vx.sort( (a,b) => { return (a.val < b.val) ?  -1 :  1} );}
 else     { kx.sort( (a,b) => { return (a.key < b.key) ?   1 : -1} ); vx.sort( (a,b) => { return (a.val < b.val) ?   1 : -1} );}
 return [kx, vx];
}

function iterateTerm(term)
{
 var ret = [];
 if (term.indexOf('{{*') == -1) ret.push(term);
 else
 {
  var t1 = term.substring(0, term.indexOf('{{*'));
  var t2 = term.substring(term.indexOf('{{*') + 3);
  var t3 = t2.substring(t2.indexOf('*}}') + 3);
  t2 = t2.substring(0, t2.indexOf('*}}'));
  var inx0 = parseInt(t2.substring(0, t2.indexOf('..')));
  var inx1 = parseInt(t2.substring(t2.indexOf('..') + 2));

  for (var i = inx0; i <= inx1 ; i++)
  {
   iterateTerm(t1 + i + t3).forEach( (t) => { ret.push(t) } )
  }
 }
 return ret;
}

function cpress(str)
{
 return lzw.compress(str, {outputEncoding: "Base64"});
}

function dpress(str)
{
 return lzw.decompress(str, {inputEncoding: "Base64"});
}

function n2(n)
{
 n = '0' + n;
 return n.substring(n.length - 2);
}

function dateString(d) { return d.getFullYear()+'-'+n2(d.getMonth()+1)+'-'+n2(d.getDate()) + ' ' + n2(d.getHours()) + ":" + n2(d.getMinutes()) + ":" + n2(d.getSeconds()) +".000"; }

function dateTime(ofs = 0) 
{ 
 var a =  dateString( new Date(new Date().getTime() + ofs)); 
return a;
}

function minUrl(url) 
{
 url = url.trim();
 url = url.split('/#')[0].trim();
 if (url.endsWith('/')) url = url.substring(0, url.length-1).trim();
 if (url.startsWith('http')) { var parts = url.replace('//', '/').split('/'); parts.shift(); url = parts.join('/'); }
 if (url.startsWith('www.')) url = url.substring(4);
 if (url.endsWith('/')) url = url.substring(0, url.length - 1);
 return url;
};

//function maxUrl(url) { if (!((url).startsWith('http'))) url= (('https://www.' + url.trim() + '/ ').replace('// ', '/').trim().replace('www.www', 'www')); return url; }
function maxUrl(url) { if (!((url).startsWith('http'))) url= (('https://www.' + url.trim()).replace('// ', '/').trim().replace('www.www', 'www')); return url; }


function streamToString (stream) 
{
 var chunks = [];
 return new Promise
 ( 
  (resolve, reject) => 
  {
   stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
   stream.on('error', (err) => reject(err));
   stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
  }
 )
}

async function write2File(fname, content)
{
 fs.writeFileSync(fname, content, function(err) { console.log(err); });
}

function showConcept()
{
 console.log('');
 console.log('spider is a task scheduler using puppeteer controlled by the postgres DB "site"');
 console.log('              CRAWLING');
 console.log("tasks of type 'crawl' will visit all the site's urls defined with 'grab=false' of a site and crawl anchors into the crawled table");
 console.log('"grab=false"-conditions and constraint like "has to contain /tag/ in the url (or the html)" can be defined with the "directive" table');
 console.log('the constraints in the "directive" table tell the crawl task which urls to visit and which urls to avoid');
 console.log('              MINING');
 console.log('mining tasks - as opposite to crawl-tasks are tasks named other than "crawl-xxxxx", e.g "google-article"');
 console.log('the "directive" table entries with "grab=true" control mining tasks defining which urls to include or to avoid');
 console.log('the idea behind is that some pages of a site will be valuable for crawling links while other pages will be valuable for mining titles and images');
 console.log('for each mining task you can define multiple html-queryselectors in the mineselectors table');
 console.log('              HTML-SELECTORS');
 console.log('queryselectors starting with "=" are used for singe line results, the other yield an array result, those must seperate name and value by ":="');
 console.log('queryselectors may also contain implicit counters like "{{*5..10*}}" to be applied several times, eg for the 5 to 10th <li>');
 console.log('              TASK SCHEDULER');
 console.log('once you have defined your carawl tasks and mining taskes these will be executed one after the other in an endless manner by invoking "node spider"');
 console.log('even if node spider does nothing for a certain period it will continue its tasks as soon as some results get outdated');
 console.log('the task table field "interval" defines for what period the results are considere up-to-date, e.g "14 days" or "1 mons 7 days"');
 console.log('              EXPORT');
 console.log('export tasks are not yet implemented');
}

function quickStart()
{
 console.log('');
 console.log('to install spider.js first setup a postgres DB "site", owner=root, pwd=root');
 console.log('in commandline do: "cd ../pb", "npm i fs", "npm i pg", "npm i lzutf8", "npm i child_process", "npm i puppeteer-core"');
 console.log('update startBrw.bat to point to your preferred chromium.exe or chrome.exe"');
 console.log('with pgAdmin restore the sample tables from the initial-DB folder, restore-type = "directory"');
 console.log('from the commandline type spider.bat - your first sample installation of the task scheduler "spider" should be running now.');
 console.log('regularly query the "crawled" table and the "mined" table to see what spider has found so far.');
}

async function killpages()
{
  return;
 pg = await brw.newPage(); // trying to avoid the situation when brw.pages() call lasts forever
 var pages = await brw.pages();
 for (var i = 1; i<= pages.length; i++)
 {
  if ((!pages[i-1].url().startsWith('https://www.binance.com')) && (!pages[i-1].url().startsWith('https://coinmarketcap.com')))
  await pages[i-1].close();
 } 
}

async function dwnld(url) 
{
 return bm.dwnld(url);
}


async function goto(url, waitForState)
{
 url = minUrl(url);
 try 
 {
  try { await Promise.all ([pg.setOfflineMode(true), pg.setOfflineMode(false), pg.goto(maxUrl(url).replace('www.', ''), {waitUntil: waitForState, timeout: 90000}), pg.waitForNavigation()]); }
  catch (err) { await Promise.all ([pg.setOfflineMode(true), pg.setOfflineMode(false), pg.goto(maxUrl(url), {waitUntil: waitForState, timeout: 90000}), pg.waitForNavigation()]); }  
  //try { await Promise.all ([pg.goto('chrome://settings/'), pg.goto(maxUrl(url).replace('www.', ''), {waitUntil: waitForState, timeout: 90000}), pg.waitForNavigation()]); }
  //catch (err) { await Promise.all ([pg.goto('chrome://settings/'), pg.goto(maxUrl(url), {waitUntil: waitForState, timeout: 90000}), pg.waitForNavigation()]); }  
 
}
 catch (err)
 {
  console.log('---- GOTO ERROR @ ' + url + ' ---- ' + err.message);
  throw err;
 }
}

async function query(sql)
{
 try 
 {
  return await pgs.query(sql);
 }
 catch (err)
 {
  if (!err.message.startsWith('doppelter'))
  {
   console.log('========== SQL ERROR ================== ' + err.message);
   console.log(sql);
  }
  throw err;
 }
}

async function init(dbStr)
{
 try {await pg.close();} catch(err){}  // await finish();
 try {await pgs.end();} catch(err){}   // await finish();
 pgs    = new pgLib({host:"localhost", port: 5432, user:"root", password:"root", database: "site"});
 var exe = "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe";
 var prod = "chrome";
 if (false)
 {
  var brwf = ppt.createBrowserFetcher ( { product: 'firefox', path: './ffx', } );
  var brwRev = await brwf.download ('102.0a1', (d, t) => console.log(`$(d) of $(t)`));
  exe = brwRev.executablePath; 
  prod = "firefox";
 }

 try 
 {
  var arg = ['--remote-debugging-port=xxx', '--disable-site-isolation-trials', '--proxy-bypass-list=*', '--disable-gpu', '--disable-dev-shm-usage', '--disable-setuid-sandbox', '--no-sandbox', '--no-zygote', '--ignore-certificate-errors', '--ignore-certificate-errors-spki-list', '--enable-features=NetworkService', '--enable-features=PasswordImport'];
  var opt = {};
  
  var exe = "browser.bat";   

  //var exe = 'call cmd /C "C:\Users\root\AppData\Local\Chromium\Application\chrome.exe" --enable-features=PasswordImport --remote-debugging-port=21222 --disable-site-isolation-trials --proxy-bypass-list=* --disable-gpu --disable-dev-shm-usage --disable-setuid-sandbox --no-sandbox --no-zygote --ignore-certificate-errors --ignore-certificate-errors-spki-list --enable-features=NetworkService';

  
  //var exe = "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"; // chrome  
  //var exe = "C:/Users/root/AppData/Local/Chromium/Application/chrome.exe"; // chromium
  //var exe = "C:/Windows/brw/opera/launcher.exe"; // opera
  //var exe = "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"; // edge
  
  var port = 21222;
  //var port = 21442;
  for (var i = 1; i <= 3; i++)
  {
   try 
   {
    var connected = true;
    const browserURL = 'http://127.0.0.1:' + port;
    try { brw = await ppt.connect({browserURL}); } catch (err) {connected = false}
    if (!connected)
    {
     var prc = await cpc.spawn(exe, arg, opt, function(err, data) { if(err) { console.log(err) } else console.log(data.toString()); })
     await delay(20000), 
     brw = await ppt.connect({browserURL});
     await prc.kill();
     await prc.kill();
     await prc.kill();
     await prc.kill();
    }
   }
   catch (err) {  console.log(err.stack); delay(10000); }; 
  }
  if (!brw) throw "no Browwer listening";

 }
/*
 try 
 {  
  brw = await ppt.launch
  (
   {
    headless:            false, 
    executablePath:      exe,
    product:             prod,'', 
    defaultViewport:     false,
    //userDir:             'C:/Users/root/AppData/Local/Google/Chrome/User Data', 
    //userDataDir:         'C:/Users/root/AppData/Local/Google/Chrome/User Data', 
    slowMo:              50,
    ignoreHTTPSErrors:   true, 
    acceptInsecureCerts: true, 
    args: 
    [
     //'--enable-features=PasswordImport', 
     '--disable-site-isolation-trials', 
     '--proxy-bypass-list=*', 
     '--disable-gpu', 
     '--disable-dev-shm-usage', 
     '--disable-setuid-sandbox', 
     //'--no-first-run', 
     '--no-sandbox', 
     '--no-zygote', 
     //'--single-process', 
     '--ignore-certificate-errors', 
     '--ignore-certificate-errors-spki-list', 
     '--enable-features=NetworkService',
    ],
   }
  );
 }

*/

 catch(err)
 {
  console.log(err);
 }
 //pg = (await brw.pages())[0];  
 brw.headless = true;
 brw.ignoreHTTPSErrors = true; 
 brw.acceptInsecureCerts = true;
 //brw.slowMo = 50;
 brw.defaultViewport = false;
 brw.product = prod;

 
 killpages(); 
 pg = await brw.newPage();
 pg.setDefaultNavigationTimeout(90000); 
 await pg.setRequestInterception(true);

 await pgs.connect();
 return [fs, brw, pg, pgs];
}

module.exports = { showConcept, quickStart, dateString, dateTime, cpress, dpress, delay, errLog, sortIx, iterateTerm, goto, query, init, finish, minUrl, maxUrl, n2, rUp, rDn, dwnld, write2File, }
