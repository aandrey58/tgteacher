import React,{useState,useEffect} from 'react';
import {Plus,ArrowUp,ArrowDown,Trash2,Image,AudioLines} from 'lucide-react';
const definitions=[
 ['Words','words',['word','translation','example','example_translation','hint']],
 ['Fill in the blank','stage2_tasks',['sentence','answer','choices','explanation']],
 ['Definitions','stage3_tasks',['word','definition','choices','explanation']],
 ['Context practice','stage4_tasks',['sentence','answer','choices','explanation']],
 ['Image recall','stage5_tasks',['image','answer','alternatives','explanation']],
 ['Synonyms','stage6_tasks',['word','synonyms','wrong_synonyms','explanation']],
 ['Listening','stage7_tasks',['task','audio','answer','choices','explanation']],
 ['Final text','stage8_tasks',['text','answers','explanations','equal']]
];
const arrays=new Set(['choices','alternatives','synonyms','wrong_synonyms','answers','explanations']);
const defaults={
 words:{word:'',translation:''},stage2_tasks:{sentence:'___',answer:'',choices:[]},stage3_tasks:{word:'',definition:'',choices:[]},
 stage4_tasks:{sentence:'___',answer:'',choices:[]},stage5_tasks:{image:'__detached__.jpg',answer:'',alternatives:[]},
 stage6_tasks:{word:'',synonyms:[],wrong_synonyms:[]},stage7_tasks:{audio:'__detached__.mp3',answer:'',choices:[]},
 stage8_tasks:{text:'___',answers:[],explanations:[]}
};
export default function TaskEditor({value,onChange,disabled=false}){
 const [stage,setStage]=useState(0),[assets,setAssets]=useState([]),[assetError,setAssetError]=useState('');
 const [name,key,fields]=definitions[stage],tasks=value[key]||[];
 useEffect(()=>{let cancel=false;setAssets([]);setAssetError('');if(stage===4||stage===6){fetch('/api/'+(stage===4?'images':'tts'),{credentials:'same-origin'}).then(async r=>{if(!r.ok)throw Error();return r.json()}).then(r=>{if(!cancel)setAssets(r.items||[])}).catch(()=>{if(!cancel)setAssetError('Unable to load media library')})}return()=>{cancel=true}},[stage]);
 const change=(field,v)=>onChange({...value,[field]:v});
 const taskChange=(index,field,v)=>change(key,tasks.map((t,i)=>i===index?{...t,[field]:v}:t));
 const move=(index,direction)=>{const n=[...tasks],to=index+direction;if(to<0||to>=n.length)return;[n[index],n[to]]=[n[to],n[index]];change(key,n)};
 return <div className="task-editor">
 <div className="editor-meta"><label>Group name<input disabled={disabled} value={value.name||''} onChange={e=>change('name',e.target.value)}/></label><label>Access<select disabled={disabled} value={value.target||'VIP+FREE'} onChange={e=>change('target',e.target.value)}>{['FREE','VIP','VIP+FREE'].map(x=><option key={x}>{x}</option>)}</select></label></div>
 <label>Description<input disabled={disabled} value={value.description||''} onChange={e=>change('description',e.target.value)}/></label>
 <div className="stage-tabs" role="tablist" aria-label="Lesson stages">{definitions.map(([label,k],i)=><button type="button" key={k} role="tab" aria-selected={stage===i} className={stage===i?'selected':''} onClick={()=>setStage(i)}><span>{i+1}</span>{label}<small>{(value[k]||[]).length}</small></button>)}</div>
 <div className="stage-heading"><h3>{stage+1}. {name}</h3><button disabled={disabled||(stage===7&&tasks.length>0)} onClick={()=>change(key,[...tasks,{...structuredClone(defaults[key]),task_id:null}])}><Plus size={15}/>Add task</button></div>
 {tasks.length===0&&<p className="empty">No tasks in this stage.</p>}
 {tasks.map((task,i)=><section className="task-card" key={i}><header><b>Task {i+1}</b><small>{task.task_id?'Permanent ID '+task.task_id:'New task'}</small><div className="task-buttons"><button disabled={disabled||i===0} aria-label={'Move task '+(i+1)+' up'} onClick={()=>move(i,-1)}><ArrowUp size={15}/></button><button disabled={disabled||i===tasks.length-1} aria-label={'Move task '+(i+1)+' down'} onClick={()=>move(i,1)}><ArrowDown size={15}/></button><button disabled={disabled} aria-label={'Remove task '+(i+1)} onClick={()=>{if(window.confirm('Remove this task from the draft? Published history remains available.'))change(key,tasks.filter((_,j)=>i!==j))}}><Trash2 size={15}/></button></div></header>
 <div className="task-fields">{fields.map(field=><label key={field}>{field.replaceAll('_',' ')}{arrays.has(field)?<textarea disabled={disabled} rows={3} placeholder="One value per line" value={(task[field]||[]).join('\n')} onChange={e=>taskChange(i,field,e.target.value.split('\n'))}/>:field==='text'?<textarea disabled={disabled} value={task[field]||''} onChange={e=>taskChange(i,field,e.target.value)}/>:<input disabled={disabled} value={task[field]||''} onChange={e=>taskChange(i,field,e.target.value)}/>}
 {(field==='image'||field==='audio')&&<><select aria-label={'Select '+field+' for task '+(i+1)} disabled={disabled} value={assets.some(a=>a.filename===task[field])?task[field]:''} onChange={e=>{if(e.target.value)taskChange(i,field,e.target.value)}}><option value="">Choose recent stored {field}</option>{assets.map(a=><option key={a.asset_id} value={a.filename}>{a.filename.slice(0,24)}…</option>)}</select>{assetError&&<small>{assetError}</small>}{assets.some(a=>a.filename===task[field])&&(field==='image'?<img className="task-preview" src={'/api/asset?kind=image&id='+assets.find(a=>a.filename===task[field]).asset_id} alt="Selected asset"/>:<audio controls src={'/api/asset?kind=audio&id='+assets.find(a=>a.filename===task[field]).asset_id}/>)}</>}
 </label>)}</div></section>)}
 </div>
}
