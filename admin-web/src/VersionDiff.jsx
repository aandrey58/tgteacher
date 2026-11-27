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
 return <section className="version-diff"><h3>Версия {historical.version} → {current.content_version}</h3><p className="muted">Сравнение сохранённых версий по постоянным ID. Заменённые задания отображаются как удалённые и добавленные.</p>
 {['name','description','target'].filter(k=>!equal(before[k],after[k])).map(k=><div className="diff-meta" key={k}><b>{{name:'Название',description:'Описание',target:'Доступ'}[k]}</b><span className="diff-old">{String(before[k]??'—')}</span><span className="diff-new">{String(after[k]??'—')}</span></div>)}
 <div className="diff-grid">{rows.map(r=><div className="diff-stage" key={r.key}><b>Этап {r.stage}</b><span className="diff-add">+{r.added.length} добавлено</span><span className="diff-remove">−{r.removed.length} удалено</span><span>{r.changed.length} изменено</span>{r.reordered&&<span>Порядок изменён</span>}</div>)}</div>
 {rows.filter(r=>r.changed.length||r.added.length||r.removed.length).map(r=><details key={r.key}><summary>Этап {r.stage} · ID изменённых заданий</summary><p>Добавлены: {r.added.map(t=>t.task_id).join(', ')||'—'}</p><p>Удалены: {r.removed.map(t=>t.task_id).join(', ')||'—'}</p><p>Изменены: {r.changed.map(t=>t.task_id).join(', ')||'—'}</p></details>)}
 </section>
}
