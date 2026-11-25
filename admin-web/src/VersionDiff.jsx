import React from 'react';

const stages=['words',...Array.from({length:7},(_,i)=>'stage'+(i+2)+'_tasks')];
const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonical(value[k])])):value;
const equal=(a,b)=>JSON.stringify(canonical(a))===JSON.stringify(canonical(b));

export function compareVersions(before,after){
 return stages.map((key,i)=>{
  const old=before[key]||[],next=after[key]||[];
  const oldById=new Map(old.map(t=>[t.task_id,t])),nextById=new Map(next.map(t=>[t.task_id,t]));
  const added=next.filter(t=>!oldById.has(t.task_id)),removed=old.filter(t=>!nextById.has(t.task_id));
  const changed=next.filter(t=>oldById.has(t.task_id)&&!equal(oldById.get(t.task_id),t));
  const oldOrder=old.filter(t=>nextById.has(t.task_id)).map(t=>t.task_id);
  const newOrder=next.filter(t=>oldById.has(t.task_id)).map(t=>t.task_id);
  return {stage:i+1,key,added,removed,changed,reordered:!equal(oldOrder,newOrder)};
 });
}
export default function VersionDiff({historical,current}){
 const before=historical.content,after=current,rows=compareVersions(before,after);
 return <section className="version-diff"><h3>Version {historical.version} → {current.content_version}</h3><p className="muted">Comparing saved versions using permanent task IDs. Replaced assessment tasks appear as removed and added.</p>
 {['name','description','target'].filter(k=>!equal(before[k],after[k])).map(k=><div className="diff-meta" key={k}><b>{k}</b><span className="diff-old">{String(before[k]??'—')}</span><span className="diff-new">{String(after[k]??'—')}</span></div>)}
 <div className="diff-grid">{rows.map(r=><div className="diff-stage" key={r.key}><b>Stage {r.stage}</b><span className="diff-add">+{r.added.length} added</span><span className="diff-remove">−{r.removed.length} removed</span><span>{r.changed.length} updated</span>{r.reordered&&<span>Order changed</span>}</div>)}</div>
 {rows.filter(r=>r.changed.length||r.added.length||r.removed.length).map(r=><details key={r.key}><summary>Stage {r.stage} · changed task IDs</summary><p>Added: {r.added.map(t=>t.task_id).join(', ')||'—'}</p><p>Removed: {r.removed.map(t=>t.task_id).join(', ')||'—'}</p><p>Updated: {r.changed.map(t=>t.task_id).join(', ')||'—'}</p></details>)}
 </section>
}
