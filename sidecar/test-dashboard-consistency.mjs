import assert from 'node:assert/strict';
import test from 'node:test';
import {DashboardConsistency,lifecycleRefreshKeys} from '../fomo/web/static/dashboard-consistency.mjs';

const now=Date.parse('2026-09-21T00:00:00Z');
const meta=(instance,dataRevision,generatedAt='2026-09-21T00:00:00Z')=>({serviceInstanceId:instance,dataRevision,generatedAt});

test('backend restart forces reload',()=>{
  const guard=new DashboardConsistency();
  assert.equal(guard.observe('identity',meta('a',18),{},now).reload,false);
  const result=guard.observe('identity',meta('b',0),{},now);
  assert.equal(result.reload,true);assert.equal(result.reason,'service_instance_changed');
});

test('registry rollback forces reload even within one service instance',()=>{
  const guard=new DashboardConsistency();
  guard.observe('identity',meta('a',1),{registryVersion:18},now);
  const result=guard.observe('identity',meta('a',2),{registryVersion:6},now);
  assert.equal(result.reload,true);assert.equal(result.reason,'domain_revision_rollback');
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
