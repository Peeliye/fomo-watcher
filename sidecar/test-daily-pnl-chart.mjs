import assert from 'node:assert/strict';
import test from 'node:test';
import {recentDailyPnl,dailyPnlGeometry} from '../fomo/web/static/daily-pnl-chart.mjs';

const row=(day,equity,change=equity)=>({day,equityUsd:equity,dailyPnlChangeUsd:change});

test('shows at most the latest fifteen recorded days in chronological order',()=>{
  const rows=Array.from({length:20},(_,index)=>row(`2026-09-${String(index+1).padStart(2,'0')}`,index));
  const selected=recentDailyPnl(rows.reverse());
  assert.equal(selected.length,15);
  assert.equal(selected[0].day,'2026-09-06');
  assert.equal(selected.at(-1).day,'2026-09-20');
});

test('shows every existing day when fewer than fifteen exist',()=>{
  assert.deepEqual(recentDailyPnl([row('2026-09-21',-2),row('2026-09-19',3)]).map(x=>x.day),['2026-09-19','2026-09-21']);
  assert.deepEqual(recentDailyPnl([{day:'2026-09-20',equityUsd:null,dailyPnlChangeUsd:1}]),[]);
});

test('chart stays within fixed bounds and bubble size follows absolute PnL',()=>{
  const chart=dailyPnlGeometry([row('2026-09-19',900,-100),row('2026-09-20',900,0),row('2026-09-21',925,25)],640);
  assert.equal(chart.points.length,3);
  assert.ok(chart.points[0].radius>chart.points[2].radius);
  assert.ok(chart.points[2].radius>chart.points[1].radius);
  assert.ok(chart.points.every(point=>point.y>=24&&point.y<=258));
  assert.ok(chart.points.every(point=>point.x>=62&&point.x<=614));
});

test('single and flat-value series have finite centered coordinates',()=>{
  for(const rows of [[row('2026-09-21',0)],[row('2026-09-20',2),row('2026-09-21',2)]]){
    const chart=dailyPnlGeometry(rows,640);
    assert.ok(chart.points.every(point=>Number.isFinite(point.x)&&Number.isFinite(point.y)&&Number.isFinite(point.radius)));
  }
});
