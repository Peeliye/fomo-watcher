const DEPENDENCIES={
  signals:['shadow','status'],orders:['orders','status'],risk:['risk','identity','status'],
  intelligence:['intelligence','performance','identity','status'],
  portfolio:['portfolio','readiness','journal','route','rpc','status'],
  rank50:['rank50','status'],wallets:['management','identity','status'],
  rpcmanager:['rpcManagement','rpc','status'],analysis:['orders','shadow','rpc','status'],
};

export function viewRefreshKeys(view){return [...(DEPENDENCIES[view]||['status'])]}

export function lifecycleRefreshKeys(view,event,visibilityState='visible'){
  if(event==='websocket_open'||(event==='visibilitychange'&&visibilityState==='visible'))return viewRefreshKeys(view);
  return [];
}

export class DashboardConsistency{
  constructor({maximumResponseAgeMs=30000,maximumFutureSkewMs=10000}={}){
    this.maximumResponseAgeMs=maximumResponseAgeMs;this.maximumFutureSkewMs=maximumFutureSkewMs;
    this.instanceId='';this.serviceStartedMs=0;this.dataRevision=null;
    this.domainRevisions=new Map();this.latestByKey=new Map();
  }
  observe(key,meta={},payload={},now=Date.now()){
    const instanceId=String(meta.serviceInstanceId||payload.serviceInstanceId||'');
    const serviceStartedMs=Date.parse(String(meta.serviceStartedAt||payload.serviceStartedAt||''));
    const revision=Number(meta.dataRevision??payload.dataRevision);
    const generatedAt=String(meta.generatedAt||payload.generatedAt||'');
    const generatedMs=Date.parse(generatedAt),ageMs=Number.isFinite(generatedMs)?now-generatedMs:Infinity;
    let resync=false,discard=false,reason='';
    if(this.instanceId&&instanceId&&instanceId!==this.instanceId){
      if(this.serviceStartedMs&&Number.isFinite(serviceStartedMs)&&serviceStartedMs<this.serviceStartedMs){
        discard=true;reason='older_service_instance';
      }else{
        resync=true;reason='service_instance_changed';
        this.dataRevision=null;this.domainRevisions.clear();this.latestByKey.clear();
      }
    }
    if(discard)return {resync:false,discard:true,reason,stale:true,ageMs};
    if(instanceId)this.instanceId=instanceId;
    if(Number.isFinite(serviceStartedMs))this.serviceStartedMs=serviceStartedMs;
    const previous=this.latestByKey.get(key);
    if(previous&&Number.isFinite(generatedMs)&&generatedMs<previous.generatedMs){
      return {resync:false,discard:true,reason:'older_response',stale:true,ageMs};
    }
    const domainRevision=Number(payload.registryVersion);
    const previousDomain=this.domainRevisions.get(key);
    if(Number.isFinite(domainRevision)&&previousDomain!=null&&domainRevision<previousDomain){
      resync=true;reason='domain_revision_rollback';
    }
    if(Number.isFinite(revision))this.dataRevision=this.dataRevision==null?revision:Math.max(this.dataRevision,revision);
    if(Number.isFinite(domainRevision))this.domainRevisions.set(key,domainRevision);
    if(Number.isFinite(generatedMs))this.latestByKey.set(key,{generatedMs,revision});
    const stale=!Number.isFinite(generatedMs)||ageMs>this.maximumResponseAgeMs||ageMs < -this.maximumFutureSkewMs;
    return {resync,discard:false,reason,stale,ageMs};
  }
}
