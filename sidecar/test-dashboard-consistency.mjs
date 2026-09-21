import assert from 'node:assert/strict';
import test from 'node:test';
import {DashboardConsistency,lifecycleRefreshKeys} from '../fomo/web/static/dashboard-consistency.mjs';

const now=Date.parse('2026-09-21T00:00:00Z');
const meta=(instance,dataRevision,generatedAt='2026-09-21T00:00:00Z',serviceStartedAt='2026-09-20T23:00:00Z')=>({serviceInstanceId:instance,dataRevision,generatedAt,serviceStartedAt});

test('backend restart resynchronizes without reloading the page',()=>{
  const guard=new DashboardConsistency();
  assert.equal(guard.observe('identity',meta('a',18),{},now).resync,false);
  const result=guard.observe('identity',meta('b',0,'2026-09-21T00:00:01Z','2026-09-21T00:00:00Z'),{},now);
  assert.equal(result.resync,true);assert.equal(result.discard,false);assert.equal(result.reason,'service_instance_changed');
  assert.equal(guard.observe('identity',meta('a',19,'2026-09-21T00:00:02Z'),{},now).discard,true);
});

test('registry rollback requests a soft resync within one service instance',()=>{
  const guard=new DashboardConsistency();
  guard.observe('identity',meta('a',1),{registryVersion:18},now);
  const result=guard.observe('identity',meta('a',2,'2026-09-21T00:00:01Z'),{registryVersion:6},now);
  assert.equal(result.resync,true);assert.equal(result.reason,'domain_revision_rollback');
  assert.equal(guard.observe('identity',meta('a',3,'2026-09-21T00:00:02Z'),{registryVersion:6},now).resync,false);
});

test('parallel API responses can have lower global revisions without causing resync',()=>{
  const guard=new DashboardConsistency();
  guard.observe('status',meta('a',40),{},now);
  const result=guard.observe('shadow',meta('a',39),{},now);
  assert.equal(result.resync,false);assert.equal(result.discard,false);
});

test('older response for the same endpoint is discarded',()=>{
  const guard=new DashboardConsistency();
  guard.observe('shadow',meta('a',12,'2026-09-21T00:00:02Z'),{},now);
  const result=guard.observe('shadow',meta('a',11,'2026-09-21T00:00:01Z'),{},now);
  assert.equal(result.discard,true);assert.equal(result.reason,'older_response');
});

test('stale and future responses are flagged',()=>{
  const guard=new DashboardConsistency({maximumResponseAgeMs:1000,maximumFutureSkewMs:1000});
  assert.equal(guard.observe('status',meta('a',1,'2026-09-20T23:59:00Z'),{},now).stale,true);
  assert.equal(guard.observe('status',meta('a',2,'2026-09-21T00:01:00Z'),{},now).stale,true);
});

test('websocket reconnect and visibility restore refresh current dependencies',()=>{
  assert.deepEqual(lifecycleRefreshKeys('risk','websocket_open'),['risk','identity','status']);
  assert.deepEqual(lifecycleRefreshKeys('portfolio','visibilitychange','visible'),['portfolio','readiness','journal','route','rpc','status']);
  assert.deepEqual(lifecycleRefreshKeys('portfolio','visibilitychange','hidden'),[]);
});
