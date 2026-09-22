export function recentDailyPnl(rows,limit=15){
  const byDay=new Map();
  for(const row of rows||[]){
    if(!/^\d{4}-\d{2}-\d{2}$/.test(String(row?.day||'')))continue;
    if(row.equityUsd==null||row.dailyPnlChangeUsd==null)continue;
    const equity=Number(row.equityUsd),change=Number(row.dailyPnlChangeUsd);
    if(!Number.isFinite(equity)||!Number.isFinite(change))continue;
    byDay.set(row.day,{...row,equityUsd:equity,dailyPnlChangeUsd:change});
  }
  return [...byDay.values()].sort((a,b)=>a.day.localeCompare(b.day)).slice(-limit);
}

export function dailyPnlGeometry(rows,width,height=300){
  const margin={left:62,right:26,top:24,bottom:42};
  const plotWidth=Math.max(1,width-margin.left-margin.right);
  const plotHeight=Math.max(1,height-margin.top-margin.bottom);
  const values=rows.map(row=>Number(row.equityUsd));
  const low=Math.min(...values),high=Math.max(...values);
  const span=high-low||Math.max(1,Math.abs(high)*0.25);
  const min=low-span*0.18,max=high+span*0.18;
  const largest=Math.max(1,...rows.map(row=>Math.abs(Number(row.dailyPnlChangeUsd))));
  const points=rows.map((row,index)=>({
    ...row,
    x:margin.left+(rows.length===1?plotWidth/2:index*plotWidth/(rows.length-1)),
    y:margin.top+(max-Number(row.equityUsd))/(max-min)*plotHeight,
    radius:6+12*Math.sqrt(Math.abs(Number(row.dailyPnlChangeUsd))/largest),
  }));
  const ticks=Array.from({length:5},(_,index)=>({
    value:max-(max-min)*index/4,
    y:margin.top+plotHeight*index/4,
  }));
  return {points,ticks,margin,plotWidth,plotHeight,min,max};
}
