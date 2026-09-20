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
    this.instanceId='';this.dataRevision=null;this.domainRevisions=new Map();
  }
  observe(key,meta={},payload={},now=Date.now()){
    const instanceId=String(meta.serviceInstanceId||payload.serviceInstanceId||'');
    const revision=Number(meta.dataRevision??payload.dataRevision);
    const generatedAt=String(meta.generatedAt||payload.generatedAt||'');
    const generatedMs=Date.parse(generatedAt),ageMs=Number.isFinite(generatedMs)?now-generatedMs:Infinity;
    let reload=false,reason='';
    if(this.instanceId&&instanceId&&instanceId!==this.instanceId){reload=true;reason='service_instance_changed'}
    if(!reload&&this.dataRevision!=null&&Number.isFinite(revision)&&revision<this.dataRevision){reload=true;reason='data_revision_rollback'}
    const domainRevision=Number(payload.registryVersion);
    const previousDomain=this.domainRevisions.get(key);
    if(!reload&&Number.isFinite(domainRevision)&&previousDomain!=null&&domainRevision<previousDomain){reload=true;reason='domain_revision_rollback'}
    if(instanceId)this.instanceId=instanceId;
    if(Number.isFinite(revision))this.dataRevision=this.dataRevision==null?revision:Math.max(this.dataRevision,revision);
    if(Number.isFinite(domainRevision))this.domainRevisions.set(key,domainRevision);
    const stale=!Number.isFinite(generatedMs)||ageMs>this.maximumResponseAgeMs||ageMs < -this.maximumFutureSkewMs;
    return {reload,reason,stale,ageMs};
  }
}
