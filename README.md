

spider is a task scheduler for web mining tasks and web crawling tasks
based on node.js and postgres and puppeteer-core.

it stores mined data and crawled links ( = page-urls) from a website 
into a postgres db according to your interval settings and to your defined 
directives and to your defined query-selectors.

PLEASE ONLY CRAWL AND MINE Websites that ALLOW such processes!

as an initial configuration sample I added a backup of an "initialDB"

for each mining task and for each crawling task you should define
what urls to mine or what urls to follow in the directives table.

the selector table defines the query selectors for each task.

the task table defines the tasks to perform in order to crawl 
and mine several websites simultanously as well as their intervals.

if you need to do it on one specific website only then this
tool may be oversized or it may be inefficient
- so you may want to use parts of this project and implement 
specific directives and selectors hard coded in your project.

so far I did not experience issues running three
spider processes locally on windows using a single database instance.

I will switch to english postgres exceptions

usage: in vscode debug spider.js
hint: with netbeans you can view and edit postgres tables

