


var pb = require('../pb');  // programming base

var fs, brw; var pg; var pgs;

async function init(dbStr)
{
 var ret = await pb.init('pgs::localhost,site,root,root'); fs = ret[0], brw = ret[1]; pg = ret[2]; pgs=ret[3];
 return [fs, brw, pg, pgs];
}

async function siteNames()
{
 var qr = await pb.query("SELECT DISTINCT site FROM site WHERE links > -1;"); 
 return qr.rows.map( (e) => e.site );
}

async function tasks()
{
 var tasks = [];
 var qr = await pb.query("SELECT DISTINCT site, url, task, interval::text FROM tasks WHERE active = true;"); 
 qr.rows.forEach( (row) => { tasks.push({ site: row.site, url: row.url, task: row.task, interval: row.interval, urlcnd: [[], [], [], []], htmlcnd: [[], [], [], []] }) }); 
 for (var i = 1; i <= tasks.length; i++) 
 {
  var task= tasks[i-1];
  if (task.task.startsWith('crawl-'))
  {
   var sql = "INSERT INTO crawled (site, url, visit, links, sshot, html) VALUES ('" + task.site + "', '" + task.url + "', '2000-01-01 00:00:00', 0, '', '' ) ";
   try { await pb.query(sql); } catch (err){}
  }  
 }
 for(var i = 1; i <= tasks.length; i++)
 {
  var qr = await pb.query("SELECT * FROM directive WHERE site = '" + tasks[i-1].site + "' AND grab = 'FALSE' AND location = 'url';");
  qr.rows.forEach ( (row) => { if (row.avoid) tasks[i-1].urlcnd[1].push(row.term); else tasks[i-1].urlcnd[0].push(row.term); } );
  var qr = await pb.query("SELECT * FROM directive WHERE site = '" + tasks[i-1].site + "' AND grab = 'TRUE' AND location = 'url';");
  qr.rows.forEach ( (row) => { if (row.avoid) tasks[i-1].urlcnd[3].push(row.term); else tasks[i-1].urlcnd[2].push(row.term);} );
  var qr = await pb.query("SELECT * FROM directive WHERE site = '" + tasks[i-1].site + "' AND grab = 'FALSE' AND location = 'html';");
  qr.rows.forEach ( (row) => { if (row.avoid) tasks[i-1].htmlcnd[1].push(row.term); else tasks[i-1].htmlcnd[0].push(row.term); } );
  var qr = await pb.query("SELECT * FROM directive WHERE site = '" + tasks[i-1].site + "'  AND grab = 'TRUE' AND location = 'html';");
  qr.rows.forEach ( (row) => { if (row.avoid) tasks[i-1].htmlcnd[3].push(row.term); else tasks[i-1].htmlcnd[2].push(row.term); });
 }
 return tasks;
}

async function createTable(table = 'all')
{
 var sql = '';

 if ((table == 'crawled') | (table == 'all'))
 {
  sql = 'CREATE UNLOGGED TABLE IF NOT EXISTS public.crawled ( site character varying COLLATE pg_catalog."default" NOT NULL, url character varying COLLATE pg_catalog."default" NOT NULL, visit timestamp without time zone NOT NULL, links bigint NOT NULL, sshot character varying COLLATE pg_catalog."default" NOT NULL, html character varying COLLATE pg_catalog."default" NOT NULL);';
  sql += 'ALTER TABLE IF EXISTS public.crawled OWNER to root;';
  sql += 'DROP INDEX IF EXISTS public.crawled_site_url;';
  sql += 'CREATE UNIQUE INDEX IF NOT EXISTS crawled_site_url ON public.crawled USING btree (site COLLATE pg_catalog."default" ASC NULLS LAST, url COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;';
  await pb.query(sql);
 }
  
 if ((table == 'mined') | (table == 'all'))
 { 
  sql = 'CREATE UNLOGGED TABLE IF NOT EXISTS public.mined(task character varying COLLATE pg_catalog."default" NOT NULL, "time" timestamp without time zone NOT NULL, name character varying COLLATE pg_catalog."default", value character varying COLLATE pg_catalog."default" NOT NULL, url character varying COLLATE pg_catalog."default" NOT NULL);';
  sql += 'ALTER TABLE IF EXISTS public.mined OWNER to root;';
  sql += 'DROP INDEX IF EXISTS public.mined_url_task_name_time;';
  sql += 'CREATE UNIQUE INDEX IF NOT EXISTS mined_url_task_name_time ON public.mined USING btree (url COLLATE pg_catalog."default" ASC NULLS LAST, name COLLATE pg_catalog."default" ASC NULLS LAST, task COLLATE pg_catalog."default" ASC NULLS LAST, "time" ASC NULLS LAST) TABLESPACE pg_default;';
  await pb.query(sql);
 }
  
 if ((table == 'selector') | (table == 'all'))
 { 
  sql = 'CREATE UNLOGGED TABLE IF NOT EXISTS public.selector(task character varying COLLATE pg_catalog."default" NOT NULL, name character varying COLLATE pg_catalog."default" NOT NULL, slc character varying COLLATE pg_catalog."default" NOT NULL) TABLESPACE pg_default;';
  sql += 'ALTER TABLE IF EXISTS public.selector OWNER to root;';
  sql += 'DROP INDEX IF EXISTS public.selector_task_name;';
  sql += 'CREATE UNIQUE INDEX IF NOT EXISTS selector_task_name ON public.selector USING btree ( task COLLATE pg_catalog."default" ASC NULLS LAST, name COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;';
  await pb.query(sql);
 }
  
 if ((table == 'directive') | (table == 'all'))
 { 
  sql = 'CREATE UNLOGGED TABLE IF NOT EXISTS public.directive ( site character varying COLLATE pg_catalog."default" NOT NULL, term character varying COLLATE pg_catalog."default" NOT NULL, location character varying COLLATE pg_catalog."default" NOT NULL, grab boolean NOT NULL, avoid boolean NOT NULL) TABLESPACE pg_default;';
  sql += 'ALTER TABLE IF EXISTS public.directive OWNER to root;';
  sql += 'DROP INDEX IF EXISTS public.cnd_site_term_location_grab;';
  sql += 'CREATE INDEX IF NOT EXISTS directive_site_term_location_grab ON public.directive USING btree (site COLLATE pg_catalog."default" ASC NULLS LAST, term COLLATE pg_catalog."default" ASC NULLS LAST, location COLLATE pg_catalog."default" ASC NULLS LAST, grab ASC NULLS LAST) TABLESPACE pg_default;';
  await pb.query(sql);
 }
 
 if ((table == 'tasks') | (table == 'all'))
 { 
  sql = 'CREATE UNLOGGED TABLE IF NOT EXISTS public.tasks (task character varying COLLATE pg_catalog."default" NOT NULL, "interval" interval NOT NULL, active boolean NOT NULL, site character varying COLLATE pg_catalog."default" NOT NULL, url character varying COLLATE pg_catalog."default" NOT NULL) TABLESPACE pg_default;';
  sql += 'ALTER TABLE IF EXISTS public.tasks OWNER to root;';
  sql += 'DROP INDEX IF EXISTS public.tasks_task_site_url;';
  sql += 'CREATE UNIQUE INDEX IF NOT EXISTS tasks_task_site_url ON public.tasks USING btree (task COLLATE pg_catalog."default" ASC NULLS LAST, site COLLATE pg_catalog."default" ASC NULLS LAST, url COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;';
  await pb.query(sql);
 }
  
  
}

function directive(sites, grab)
{
 var ret = "";
 var inx = grab ? 2 : 0;
 
 var cnd = [];
 sites[0].urlcnd[inx].forEach    ( term => term.startsWith('! ') ? cnd.push(" CONCAT(url, '/') NOT ILIKE '%" + term.substring(2) + "%' ") : cnd.push(" CONCAT(url, '/') ILIKE '%" + term + "%' ") ); 
 sites[0].htmlcnd[inx].forEach   ( term => term.startsWith('! ') ? cnd.push(" LENGTH(html) > 0 AND html NOT LIKE '%" + term.substring(2) + "%' ") : cnd.push(" html LIKE '%" + term + "%' ") ); 
 
 if (cnd.length > 0)
 {
  ret += " AND ( (url = site) ";
  cnd.forEach( term => ret += " OR ( " + term + ") ");
  ret += " ) ";
 }
 cnd = [];
 sites[0].urlcnd[1+inx].forEach  ( term => term.startsWith('! ') ? cnd.push(" CONCAT(url, '/') ILIKE '%" + term.substring(2) + "%' ") : cnd.push(" CONCAT(url, '/') NOT ILIKE '%" + term + "%' ") ); 
 sites[0].htmlcnd[1+inx].forEach ( term => term.startsWith('! ') ? cnd.push(" html LIKE '%" + term.substring(2) + "%' ") : cnd.push(" LENGTH(html) > 0 AND html NOT LIKE '%" + term + "%' ") ); 
 if (cnd.length > 0) cnd.forEach( term => ret += " AND ( " + term + ") ");
 return ret; 
}

async function url2crawl(tasks)
{
 var sql = "SELECT site, url, visit, links FROM crawled WHERE site = '" + tasks[0].site + "' AND visit <  (NOW() - interval '" + tasks[0].interval + "') "; 
 sql += directive(tasks, false);
 sql += " ORDER BY visit, LENGTH(url) LIMIT 10;"; 
 var qr = await pb.query (sql); 
 if (qr.rows.length > 0) return qr.rows[(1 * (new Date())) % qr.rows.length].url;
 return '';
}

async function url2mine(tasks)
{
 var sql = "SELECT site, url, visit, links FROM crawled WHERE site = '" + tasks[0].site + "' AND visit <  (NOW() - interval '" + tasks[0].interval + "') "; 
 sql += directive(tasks, true);
 sql += " ORDER BY visit, LENGTH(url) LIMIT 10;"; 
 var qr = await pb.query (sql); 
 if (qr.rows.length > 0) return qr.rows[(1 * (new Date())) % qr.rows.length].url;
 return '';
}
 

async function stats(tasks, grab)
{
 var done = Math.floor(Object.values((await pb.query("SELECT COUNT(*) FROM crawled WHERE site = '" + tasks[0].site + "' AND visit >= (NOW() - interval '" + tasks[0].interval + "') " + directive(tasks, grab))).rows[0])[0]);
 var todo = Math.floor(Object.values((await pb.query("SELECT COUNT(*) FROM crawled WHERE site = '" + tasks[0].site + "' AND visit <  (NOW() - interval '" + tasks[0].interval + "') " + directive(tasks, grab))).rows[0])[0]);
 return  (100 * (done / (todo + done))).toFixed(1) + "% (" + done + "/" + (todo + done) + ")";
}


async function addPage(site, url)
{
 try 
 {
  var sql = "INSERT INTO crawled (site, url, visit, links, sshot, html) values('" + site + "', '" + pb.minUrl(url) + "', '2000-01-01 00:00:00', 0, '', '');";
  await pb.query(sql);
  console.log('-->db ' + url);
  return 1;
 }
 catch(err) { return 0; }
}



async function createView(path)
{
  var res; var qr; var sql = ""; var directive = ""; var total;
  res = fs.readFileSync((path + '/dataView.html').replaceAll('//', '/'), {  }, function(err) { console.log(err); }).toString().split('var data ='); 
  res[0] = res[0] + 'var data = \r\n[\r\n';
  res[1] = '\r\n];\r\n' + res[1].split('];')[1];
  res.push([]);
  sql = "SELECT SUBSTRING(name, 0, POSITION('-' IN name)) AS name, CONCAT(TRIM(value)::DECIMAL, ' ', TO_CHAR(time, 'HH24:mm:ss')) as reading FROM mined WHERE ((task='coinmarketcap-coinval') OR (task='binance-value')) AND TRIM(value) <> '0' AND time > (NOW() - interval '500' minute) AND time <= (NOW() - interval '1' second) AND name LIKE ('%-Value') ORDER BY name, time";
  var qr = await pb.query(sql);
  console.log("FOUND " + qr.rows.length + " Data Entries");
  path = (path + '/_' + pb.dateTime().replaceAll(':', '´').replaceAll(' ', '_')).replaceAll('//', '/');
  try { fs.mkdirSync(path)               } catch(err) {}
  var symb = "";
  var rowData = [];
  qr.rows.forEach
  ( 
   (row) => 
   {
    if (row.name == symb) rowData[0].push("'" + row.reading + "'");
    else 
    {
     symb = row.name;
     rowData.push(["'" + symb + "'", "'" + row.reading + "'"]);     
     pb.rDn(rowData); 
    }
   }
  )
  rowData.forEach
  ( 
   (row) => 
   {
    symb = row[0];
    while (row.length > 12) row.shift();
    row.push(symb);
   }
  )
  rowData.forEach( (row) => res[2].push('[' + row.join(', ') +']'));
  fs.writeFileSync(path + '/dataView.html', res[0] + res[2].join(',\r\n') + res[1], function(err) { console.log(err); }); 
}


async function expResult(folder, site, blocksize = 3000)
{
 var res; var qr; var path = ""; var sql = ""; var directive = ""; var total;

 res = fs.readFileSync((folder + '/spiderResultList.html').replaceAll('//', '/'), {  }, function(err) { console.log(err); }).toString().split('var data ='); 
 res[0] = res[0] + 'var data = \r\n[\r\n';
 res[1] = '\r\n];\r\n' + res[1].split('];')[1];
 res.push([]);

 directive = await directive(site);
 sql = "SELECT COUNT(*) AS total FROM crawled WHERE site = '" + site + "' AND SUBSTRING(html, 0, 5000) LIKE '%og:task%content%article%' AND LENGTH(sshot) > 0 AND LENGTH(html) > 0 " + directive;
 total = (await pb.query(sql)).rows[0].total; 
 console.log("found " + total + " valid pages for " + site);

 for (var i = 1; i <= 1 + total/blocksize; i++)
 {
  res[2] = [];
  console.log(" ---------- getting pages " + (1 + blocksize * (i - 1)) + ' to ' + blocksize * i);
  sql = "SELECT SUBSTRING(html, 19 + POSITION('og:title' IN SUBSTRING(html, 0, 5000)), -1 + POSITION ('\">' IN SUBSTRING(html, 19 + POSITION('og:title' IN SUBSTRING(html, 0, 5000))))) AS title, url, TO_CHAR(visit, 'YYYY-MM-DD') AS visit, links, sshot, md5(sshot) AS md5 FROM crawled WHERE site = '" + site + "' AND SUBSTRING(html, 0, 5000) LIKE '%og:task%content%article%' AND LENGTH(sshot) > 0 AND LENGTH(html) > 0 " + directive + " ORDER BY visit OFFSET " + blocksize * (i - 1) + " LIMIT " + blocksize + ";";
  var qr = await pb.query(sql);
  console.log("FOUND " + qr.rows.length + " PAGES");
  path = (folder + '/_' + site + '.' + i).replaceAll('//', '/');
  qr.rows.forEach
  ( 
   (row) => 
   {
    try { fs.mkdirSync(path)               } catch(err) {}
    try { fs.mkdirSync(path + '/sshot')    } catch(err) {}
    console.log('exporting ' + (path + '/sshot/' + row.md5).replaceAll('//', '/') + '.jpg');
    fs.writeFileSync((path + '/sshot/' + row.md5).replaceAll('//', '/') + '.jpg', Buffer.from(row.sshot.toString(), 'base64'), { flag: 'w' }, function(err) { console.log(err); }); 
    res[2].push("['" + row.visit + "', '" + row.title.replaceAll("'", "\\'") + "', '" + row.url + "', '" + row.md5 + "']");
   }
  )
  fs.writeFileSync(path + '/spiderResultList.html', res[0] + res[2].join(',\r\n') + res[1], function(err) { console.log(err); }); 
 } 
}

async function expSshot(folder, site)
{
  var path;
  var file;
  var sql = "SELECT site, sshot, MD5(sshot) AS md5 FROM crawled WHERE site = '" + site + "' AND LENGTH(sshot) > 0 AND LENGTH(html) > 0 ";
 
  var qr = await pb.query("SELECT * FROM directive WHERE site = '" + site + "' AND grab = false AND avoid = false;");
  if (qr.rows.length > 0)
  {
   sql += ' AND ( (site = url) ';
   qr.rows.forEach( (row) => sql += " OR ( html ILIKE '%" + row.term + "%') " );
   sql += ' ) ';
  }
  qr = await pb.query("SELECT * FROM directive WHERE site = '" + site + "' AND grab = true AND avoid = true;");
  if (qr.rows.length > 0) qr.rows.forEach ( (row) => sql += " AND ( html NOT ILIKE '%" + row.term + "%') " );
 
  var qr = await pb.query(sql);
  qr.rows.forEach
  ( 
   (row) => 
   {
    path = (folder + '/_' + row.site + '/sshot/' + row.md5).replaceAll('//', '/');
    console.log('exporting ' + path);
    fs.writeFileSync(path + '.jpg', Buffer.from(row.sshot.toString(), 'base64'), { flag: 'w' }, function(err) { console.log(err); }); 
  }
  )
 }
 
async function expHtml(folder, site)
{
 var path;
 var file;
 var sql = "SELECT site, html, MD5(sshot) AS md5 FROM crawled WHERE site = '" + site + "' AND LENGTH(sshot) > 0 AND LENGTH(html) > 0 ";

 var qr = await pb.query("SELECT * FROM cndhtml WHERE site = '" + site + "' AND avoid = false;");
 if (qr.rows.length > 0)
 {
  sql += ' AND ( (site = url) ';
  qr.rows.forEach( (row) => sql += " OR ( html ILIKE '%" + row.term + "%') " );
  sql += ' ) ';
 }
 qr = await pb.query("SELECT * FROM cndhtml WHERE site = '" + site + "' AND avoid = true;");
 if (qr.rows.length > 0) qr.rows.forEach ( (row) => sql += " AND ( html NOT ILIKE '%" + row.term + "%') " );

 var qr = await pb.query(sql);
 qr.rows.forEach
 ( 
  (row) => 
  {
   path = (folder + '/_' + row.site + '/html/' + row.md5).replaceAll('//', '/');
   console.log('exporting ' + path);
   fs.writeFileSync(path + '.html', row.html.toString(), { flag: 'w' }, function(err) { console.log(err); }); 
  }
 )
}


async function img2file(url, path)
{
 var img = Object.values((await pb.query("SELECT sshot FROM crawled WHERE url = '" + url + "';")).rows[0])[0].toString();
  //await pb.query("insert into bytes values('" + img.toString('base64') + "')");
  //img = Object.values((await pb.query("select * from bytes")).rows[0])[0].toString();
  await fs.writeFileSync(path, Buffer.from(img, 'base64'), { flag: 'w' }, function(err) { console.log(err); });
}

async function visited(site, pageUrl, ctr, img = null, html = '')
{
 var sql = "";
 if (img == null)
  sql = "UPDATE crawled SET visit = '" + pb.dateTime() + "', links = " + ctr + ", sshot = '' WHERE site = '" + site + "' AND url = '" + pb.minUrl(pageUrl) + "';";
 else 
  sql = "UPDATE crawled SET visit = '" + pb.dateTime() + "', links = " + ctr + ", html = '" + html.substring(0, 8100).replaceAll("'", "''") + "', sshot = '" + img + "' WHERE site = '" + site + "' AND url = '" + pb.minUrl(pageUrl) + "';";
 await pb.query(sql);
}

module.exports = { init, url2crawl, url2mine, addPage, visited, tasks, img2file, stats, expResult, expSshot, expHtml, createView, createTable, directive, }






