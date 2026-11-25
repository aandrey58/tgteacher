import React from 'react';
import {Clock,CheckCircle2,RotateCcw,Layers,FileText,Users,CalendarCheck,Image,AudioLines,Database,ShieldCheck,ArrowRight,BookOpen,PlayCircle} from 'lucide-react';

export function formatDate(value){
 if(!value)return '—';
 const d=new Date(value);
 return Number.isNaN(d.getTime())?String(value):d.toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'});
}
const metrics={
 published:[Layers,'Published groups'],published_groups:[Layers,'Published groups'],archived:[Layers,'Archived groups'],archived_groups:[Layers,'Archived groups'],
 open_drafts:[FileText,'Open drafts'],drafts:[FileText,'Drafts'],edit_drafts:[FileText,'Edit drafts'],published_drafts:[CheckCircle2,'Published drafts'],
 active_learners:[Users,'Active learners · 7 days'],total_learners:[Users,'Total learners'],new_learners:[Users,'New learners · 7 days'],vip_learners:[ShieldCheck,'VIP learners'],
 words:[BookOpen,'Active words'],versions:[Clock,'Content versions'],due_cards:[CalendarCheck,'Due cards'],active_cards:[CalendarCheck,'Active cards'],rated_cards:[CheckCircle2,'Rated cards'],reviewed_today:[CheckCircle2,'Reviews today · UTC'],
 audio_assets:[AudioLines,'Audio assets'],image_assets:[Image,'Image assets'],historical_audio_links:[AudioLines,'Version-linked audio'],historical_image_links:[Image,'Version-linked images'],
 ready_requests:[CheckCircle2,'Ready requests'],pending_requests:[Clock,'Pending requests'],failed_requests:[RotateCcw,'Failed requests'],TTS:[AudioLines,'TTS reserved today'],Images:[Image,'Images reserved today']
};
export function MetricCards({data,page}){
 if(!data)return null;
 return <div className="metrics">{Object.entries(data).map(([key,v],i)=>{const [Icon,label]=metrics[key]||[Database,key.replaceAll('_',' ').replace(/^./,x=>x.toUpperCase())];return <section key={key} className={'metric tint'+i%4}><div className="metric-label"><Icon size={21}/>{label}</div><strong>{page==='budgets'?'$'+(v/1e6).toFixed(4):typeof v==='number'?v.toLocaleString():v}</strong><small>{page==='budgets'?'Reserved · not invoiced':'Current database totals'}</small></section>})}</div>
}
export function PublishingActivity({items=[],open,go}){
 return <section className="panel publishing"><h2><Clock size={20}/>Recent publishing activity</h2>{items.length?<div className="timeline">{items.map(v=><button className="timeline-item" key={v.family_id+'-'+v.version} onClick={()=>open({id:v.family_id,name:v.name})}><span className="timeline-icon">{v.restored_from?<RotateCcw size={16}/>:<CheckCircle2 size={17}/>}</span><span><b>{v.name}</b><small>Version {v.version}{v.restored_from?' · restored v'+v.restored_from:' · published'}</small><small>{formatDate(v.created_at)}</small></span><ArrowRight size={14}/></button>)}</div>:<p className="empty">No published versions yet.</p>}<button className="link-button" onClick={()=>go('groups')}>View groups <ArrowRight size={15}/></button></section>
}
const railCopy={drafts:['Draft workflow','Save changes as a revision, validate the saved content, then publish. Existing learner history remains intact.'],learners:['Learner access','VIP is based on current subscription status. Activity reflects recorded usage, not predicted retention.'],reviews:['FSRS workload','Due cards are counted from the current schedule. Rated cards are not a measure of recall accuracy.'],media:['Immutable media','Generated assets remain available to historical versions. Open an asset to see its provenance and usage.'],tts:['Speech generation','Choose an enabled model and voice, estimate the cost, then approve generation. Cached results do not require another paid request.'],images:['Image generation','Images are normalized to JPEG and stored by content hash. Choose a model configured by the operator.'],budgets:['Budget rules','Daily limits reset at UTC midnight. Failed or pending requests retain reservations to avoid double billing.'],settings:['Security','The web session is independent of MCP and learner credentials. Configuration secrets stay on the server.']};
export function WorkspaceRail({page,data,open,go}){
 const copy=railCopy[page]||['Content management','Build groups with all eight lesson stages. Publish through a validated draft and retain earlier versions.'];
 return <aside className="right-rail">{page==='groups'?<PublishingActivity items={data.publishing} open={open} go={go}/>:<section className="panel"><h2><ShieldCheck size={20}/>{copy[0]}</h2><p className="muted">{copy[1]}</p></section>}<section className="panel accent"><div className="rail-art"><BookOpen size={33}/><span><Layers size={27}/></span></div><h2>{page==='groups'?'Create high-quality word groups':'TGTeacher workspace'}</h2><p>{page==='groups'?'Build, validate and publish English vocabulary at scale.':'Your changes are saved to the shared PostgreSQL database.'}</p><small><CheckCircle2 size={14}/> Versioned content</small><small><CheckCircle2 size={14}/> Protected learner history</small>{page==='groups'&&<button className="link-button" onClick={()=>go('drafts')}>Open drafts <ArrowRight size={15}/></button>}</section></aside>
}
export function AssetGallery({rows,open}){
 return <div className="asset-gallery">{rows.map(r=><button className="asset-card" key={r.kind+r.asset_id} onClick={()=>open(r)}><div className={'asset-visual '+r.kind}>{r.kind==='image'?<img loading="lazy" alt="Generated lesson image" src={'/api/asset?kind=image&id='+encodeURIComponent(r.asset_id)}/>:<><AudioLines size={48}/><PlayCircle size={25}/></>}</div><div className="asset-info"><b>{r.filename.slice(0,20)}…</b><span>{r.kind==='image'?r.width+' × '+r.height:'MP3'} · {Math.ceil(r.size_bytes/1024)} KB</span><small>{formatDate(r.created_at)}</small><span className="asset-link">Open asset <ArrowRight size={14}/></span></div></button>)}</div>
}
