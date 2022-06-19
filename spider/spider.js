

var ret, fs, pgs, brw, pg;
var pb = require('../pb'); // programming base
var db = require('../db'); // database
var cm = require('../cm'); // crawler modules

async function execSlc(tasks)
{
 var ret = [];
 var sql = "SELECT name, slc FROM selector s WHERE task = '" + tasks[0].task + "'";
 var qr = Object.values((await pb.query(sql)).rows);
 var pgRes = "";
 
 for(var i = 1; i <= qr.length; i++)
 { 
  var terms = pb.iterateTerm(qr[i-1].slc);
  for (var j = 1; j <= terms.length; j++)
  {
   pgRes = []; 
   try 
   {
    if (terms[j-1].startsWith("=")) pgRes.push(await pg.evaluate( terms[j-1].substring(1, 100000))); else pgRes = await pg.evaluate( terms[j-1], (El) => El.map((el) => el.toString()));
   }
   catch (err) { console.log(err.stack); }
   for (var k = 1; k <= pgRes.length; k++)
   {
    var name = qr[i-1].name;
    value = '';
    try { value = pgRes[k-1].toString(); } catch (err) {}
    if (value.length > 0)
    {
     if (qr[i-1].name.startsWith('-')) { name =  value.split(':=')[0] + qr[i-1].name; value = value.split(':=')[1]; }  
     if (value != undefined) ret.push({name: name, value: value}); 
    }
   }
  };
 }
 return ret;
}
 
async function mineData(tasks)
{
 var time = pb.dateTime();
 //var sql = "SELECT m.url, m.amount, s.name, s.slc FROM mineSites m, selector s WHERE m.task = s.task AND m.task = '" + tasks[0].task + "' GROUP BY m.url, m.amount, s.name, s.slc";
 var url = await db.url2mine(tasks);
 if (url.length == 0) return;
 console.log(await db.stats(tasks, true) + ' <MINING> TASK = ' + tasks[0].task + ' @ ' + url); //tasks[0].site
 await pb.goto(url, "networkidle2"); // "load" "domcontentloaded" "networkidle0" "networkidle2"
 await db.visited(tasks[0].site, url, 0, '', (await pg.content()));
 //await pg.addScriptTag( { url: "https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js"} );
 //await pg.addScriptTag( { content:'jQuery(function($){})' });
 //await pg.addScriptTag( { content: "(function(){})(jQuery)"} );
 //if (url != pb.minUrl(pg.url()) && url + '/' != pb.minUrl(pg.url()) && url != pb.minUrl(pg.url() + '/')) return;
 if (tasks[0].task.startsWith('!'))
 {
  //var html = ;
  //await pb.query("UPDATE crawled SET html = '" + html + "' WHERE site = '" + tasks[0].site + "' AND url = '" + tasks[0].url + "'");
 }
 else await crawlAnchors(tasks, url, false);
 if (url != pb.minUrl(pg.url()) && url + '/' != pb.minUrl(pg.url()) && url != pb.minUrl(pg.url() + '/')) return;
 var res = await execSlc(tasks);
 for (var i = 1; i <= res.length; i++)
 { 
  console.log(" <= " + res[i-1].name + " := " + res[i-1].value);
  var sql = "INSERT INTO mined (url, task, time, name, value) values('" + url + "', '" + tasks[0].task + "', '" + time + "' , '" + res[i-1].name + "', '" + res[i-1].value.replaceAll("'", "''") + "');";
  try { await pb.query(sql); } catch (err) {console.log(err.message)}  
 }
}

async function crawlAnchors(tasks, pageUrl, loadPage)
{
 var time = pb.dateTime();
 await db.visited(tasks[0].site, pageUrl, 0);
 console.log((await db.stats(tasks, false)) + ' <CRAWLING> ' + pageUrl);
 if (loadPage) await pb.goto(pageUrl, "networkidle2"); // "load" "domcontentloaded" "networkidle0" "networkidle2"
 if (pageUrl != pb.minUrl(pg.url()) && pageUrl + '/' != pb.minUrl(pg.url()) && pageUrl != pb.minUrl(pg.url() + '/')) // redirection occurred
 {
  console.log(' REDIRECTED BY BROWSER: ' + pageUrl +' != ' + pb.minUrl(pg.url()));
  //return; 
 }
 var img = '';  //var img = await pg.screenshot({task: 'jpeg', fullPage: true, encoding: 'binary', quality: 20});
 var ctr = 0;
 var res = await execSlc(tasks);
 for (var i = 1; i <= res.length; i++)
 {  
  var url = res[i-1].value;
  if (!url) continue;
  url = url.trim();
  if (url.startsWith('/')) url = pb.maxUrl(pg.url().split('/')[2] + url);
  else if (url.startsWith('./')) url = pg.maxurl(pg.url().split('//')[0] + '//' + (pg.url().split('//')[1] + url.substring(1)).replace('//', '/'));
   else if (url.startsWith('#')) continue;  //url = (pg.url() + "/ ").replace('// ', '/').trim();
  if (url.startsWith('http') || url.startsWith(tasks[0].site)) 
  {
   url = pb.minUrl(url);  
   if ((url.indexOf('undefined') == -1) && (url.indexOf('/?') + url.indexOf('/&') + url.indexOf('/%C2') + url.indexOf('/#') < 0) && (url.startsWith(tasks[0].site)) && !url.endsWith('.pdf'))  
    try { ctr += await db.addPage(tasks[0].site, url); } catch(err) {} 
  }
 }
 try
 {
  var html = await pg.content();
  await db.visited(tasks[0].site, pageUrl, res.length, img.toString('base64'), html);
  //await db.img2file(pageUrl, '22.jpg'); // just testing if the right image is in db.
 }
 catch(err) { throw err } //maybe some network error or page is down // visit again in future
}

var tasks = [];

async function test()
{
 //var res = pb.iterateTerm(" a{{*5..6*}}  b{{*0..3*}}  c{{*-4..-2*}}  "); // 24 results
 //pb.dwnld("https://www.ikea.at")


 //process.argv.push('--createTable');
 //process.argv.push('crawled');
 
 
 return;
 await pb.query("delete from bytes");
 await pb.query("insert into bytes values('ABCDE')");
 var qr = await pb.query('select * from bytes');
 console.table(Object.values(qr.rows[0])[0]);
}

async function genRss(task, start, end)
{

 var sql = "SELECT DISTINCT time, url, name, value FROM mined WHERE LENGTH(value) > 0 AND url IN (SELECT DISTINCT url FROM mined WHERE  time >= '" + start + "' AND time <= '" + end + "' AND task = 'ambcrypto-article' AND name = 'ImageUrl') ORDER BY url, time DESC, name DESC;"
 var qr = await pb.query(sql);
 var html = "<html><body><h><center>" + task + "s found from 2022-06-14' to '2022-06-16'  </h>";
 for (var i = 1; i <= qr.rows.length; i++)
 {
  var trow = qr.rows[i-1];
  if (trow.name != 'Title') continue;
  var url = trow.url;
  var irow = qr.rows[i];
  if ((url != irow.url) || (irow.name != 'ImageUrl')) continue;
  var drow = qr.rows[i+1];
  if ((url != drow.url) || (drow.name != 'Description')) continue;
  html += "<div><font size=3><center><br><b>" + trow.value + "</b><br><br></center></font><a target=_blank href='https://" + url + "'><img width=400 src = '" + irow.value + "'></img></a><br><br>" + drow.value + "</div> "
 } 
 html += "</body></html>";
 await pb.write2File('result.html', html); 
  
 var rss  = "";
 rss += "<?xml version=\"1.0\" ?>";
 rss += "<rss version=\"2.0\">";
 rss += "    <channel>";
 rss += "      <title>" + task + "</title>";
 //rss += "      <link>https://www.xul.fr/en/</link>";
 //rss += "      <description>XML graphical interface etc...</description>";
 //rss += "      <image>";
 //rss += "          <url>https://www.xul.fr/xul-icon.gif</url>";
 //rss += "          <link>https://www.xul.fr/en/index.php</link>";
 //rss += "      </image>";
 rss += "      <item>";
 rss += "          <title>News  of today</title>";
 rss += "          <link>https://www.xul.fr/en-xml-rss.html</link>";
 rss += "          <description>All you need to know about RSS</description>";
 rss += "      </item>";
 rss += "      <item>";
 rss += "          <title>News of tomorrows</title>";
 rss += "          <link>https://www.xul.fr/en-xml-rdf.html</link>";
 rss += "          <description>And now, all about RDF</description>";
 rss += "        </item>";
 rss += "    </channel>";
 rss += "    </rss>";
     

}

(async function main()
{
 // var test = [ {'b':'z'}, {'a':'y'}, {'c':'x'}]; var [kx, vx] = pb.sortIx(test); console.table(kx); console.table(vx); for( var i = 1; i<=test.length; i++)  console.log(test[kx[i-1].ix-1]);

 await test();

 console.log('Usage:');
 console.log(' node spider                    ... do all the tasks scheduled in tasks table');
 console.log(' node spider --concept          ... shows the general idea how spider is supposed to work');
 console.log(' node spider --quickStart       ... shows quick installation instructions');
 console.log(' node spider --createTable all  ... creates all tabes in an empty site-DB');
 console.log('');
 ret = await cm.init('pgs::localhost,site,root,root'); fs = ret[0]; brw = ret[1]; pg = ret[2]; pgs=ret[3]; 

 await genRss('ambcrypto-article', '2022-06-17 00:00:00', '2022-06-17 23:59:59');

 if (process.argv.length > 2)
 {
  switch (process.argv[2])
  {
   case '--createTable' : await db.createTable(process.argv[3])  ; break;
   case '--concept'     : await pb.showConcept()                 ; break;
   case '--quickStart'  : await pb.quickStart()                  ; break;
  }
  await pb.delay(15000);
  process.abort();
 }
 //await db.createTable('all');

 //await db.expResult('./_xl/', 'ambcrypto.com');
 

 //await db.expSshot('./_xl/', 'ambcrypto.com');
 //await db.expHtml('./_xl/', 'ambcrypto.com');
 //await db.expSshot('./_xl/', 'coindesk.com');
 //await db.expHtml('./_xl/', 'coindesk.com');
 //await db.expSshot('./_xl/', 'artmiami.com');
 //await db.expHtml('./_xl/', 'artmiami.com'); 

 //await db.expSshot('./_xl/', 'wogibtswas.clsat');
 //await db.expHtml('./_xl/', 'wogibtswas.at');

 //await db.expSshot('./_xl/', 'marktguru.at');
 //await db.expHtml('./_xl/', 'marktguru.at');

 //await pb.query("UPDATE crawled SET visit = '2000-01-01 00:00:00' WHERE LENGTH(html) = 0");
 while (true)
 {
  //pb.delay(2000);
  
  try
  {   
   pg.on
   (
    'request', rq => 
    { 
     //console.log('       resource ' + rq.resourceType());
     if (rq.resourceType() === 'image' || rq.resourceType() === 'font') 
     rq.abort(); 
     else 
     rq.continue() 
    }
   )  //pg.on ('request', (rq) => rq.continue());
   
   pg.on ('resptasksonse', (rs) => {  });
   tasks = await db.tasks();
   var done = false;
   var ctr = 0;
   while (!done) 
   {
    try
    {
//     if (ctr++ % 10 == 0) await db.createView('./_xl/'); 
     var qr = null;
     pb.rUp(tasks);
     if (!tasks[0].task.startsWith('crawl-'))
     {
      await mineData(tasks);   
     }
     else
     {
      var url = await db.url2crawl(tasks); 
      if (url > '') await crawlAnchors(tasks, url, true);
     }
    }
    catch (err)
    {
     throw err;
     //console.log(err.stack); 
    } 
   }
  }
  catch(err) 
  {
   try //troubleshooting 
   {
    console.log('===========>> ' + err.message);
    await pb.delay(10000);
         if (err.message.startsWith('net::ERR_NAME_NOT_RESOLVED')         ) { console.log(err); }
    else if (err.message.startsWith('Execution context was destroyed')    ) { console.log('trying pg.reload'); await pg.reload({ waitUntil: ["networkidle0", "domcontentloaded"] }); }
    else if (err.message.startsWith('Navigation timeout of')              ) { console.log('trying pg.reload'); await pg.reload({ waitUntil: ["networkidle0", "domcontentloaded"] }); }
    else if (err.message.startsWith('net::ERR_NAME_NOT_RESOLVED')         ) { console.log(err); }
    else 
    try 
    {
     console.log(' TROUBLESHOOTING COULD NOT FIX ===========>> ' + err.message);
     console.log(err.stack);
     console.log(" =================== RESTARTING BROWSER AND DATABASE =================== "); 
     await pb.delay(3000);
     try { ret = await cm.init('pgs::localhost,site,root,root'); fs = ret[0]; brw = ret[1]; pg = ret[2]; pgs=ret[3]; }
     catch(err) {console.log(err.stack)} 
    }
    catch (err) {console.log(err.stack)}    
    await pb.delay(20000);
   }
   catch (err)
   {
    console.log(' TROUBLESHOOTING COULD NOT FIX ===========>> ' + err.message);
    console.log(err.stack);
    console.log(" =================== RESTARTING BROWSER AND DATABASE =================== "); 
    console.log(" =================== RESTARTING BROWSER AND DATABASE =================== "); 
    console.log(" =================== RESTARTING BROWSER AND DATABASE =================== "); 
    await pb.delay(3000);
    try { ret = await cm.init('pgs::localhost,site,root,root'); fs = ret[0]; brw = ret[1]; pg = ret[2]; pgs=ret[3]; }
    catch(err) {console.log(err.stack)} 
   } 
  }
 }
} 
)();

console.log('Startup done.');

//setTimeout(finish, 30000);
