

import scrape from 'website-scraper';



async function dwnld(url)
{
 var options = { urls:["https://www.ikea.at"], directory: 'd:/ikea'}
 var ret  = await scrape( options );
}

export default dwnld;
