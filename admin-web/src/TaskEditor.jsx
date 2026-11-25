import React,{useState,useEffect} from 'react';
import {Plus,ArrowUp,ArrowDown,Trash2,Image,AudioLines} from 'lucide-react';
const definitions=[
 ['Слова','words',['word','translation','example','example_translation','hint']],
 ['Заполнение пропусков','stage2_tasks',['sentence','answer','choices','explanation']],
 ['Определения','stage3_tasks',['word','definition','choices','explanation']],
 ['Слова в контексте','stage4_tasks',['sentence','answer','choices','explanation']],
 ['Слова по картинке','stage5_tasks',['image','answer','alternatives','explanation']],
 ['Синонимы','stage6_tasks',['word','synonyms','wrong_synonyms','explanation']],
 ['Аудирование','stage7_tasks',['task','audio','answer','choices','explanation']],
 ['Итоговый текст','stage8_tasks',['text','answers','explanations','equal']]
];
const fieldLabels={word:'Слово',translation:'Перевод',example:'Пример',example_translation:'Перевод примера',hint:'Подсказка',sentence:'Предложение',answer:'Ответ',choices:'Варианты ответа',explanation:'Объяснение',definition:'Определение',image:'Изображение',alternatives:'Допустимые ответы',synonyms:'Синонимы',wrong_synonyms:'Неверные синонимы',task:'Задание',audio:'Аудио',text:'Текст',answers:'Ответы',explanations:'Объяснения',equal:'Равнозначные пропуски'};
const arrays=new Set(['choices','alternatives','synonyms','wrong_synonyms','answers','explanations']);
const defaults={
 words:{word:'',translation:''},stage2_tasks:{sentence:'___',answer:'',choices:[]},stage3_tasks:{word:'',definition:'',choices:[]},
 stage4_tasks:{sentence:'___',answer:'',choices:[]},stage5_tasks:{image:'__detached__.jpg',answer:'',alternatives:[]},
 stage6_tasks:{word:'',synonyms:[],wrong_synonyms:[]},stage7_tasks:{audio:'__detached__.mp3',answer:'',choices:[]},
 stage8_tasks:{text:'___',answers:[],explanations:[]}
};
export default function TaskEditor({value,onChange,disabled=false}){
 const [stage,setStage]=useState(0),[assets,setAssets]=useState([]),[assetError,setAssetError]=useState(''),[assetQuery,setAssetQuery]=useState(''),[assetOffset,setAssetOffset]=useState(0),[assetNext,setAssetNext]=useState(false);
 const [name,key,fields]=definitions[stage],tasks=value[key]||[];
 useEffect(()=>{let cancel=false;setAssets([]);setAssetError('');setAssetNext(false);if(stage===4||stage===6){fetch('/api/'+(stage===4?'images':'tts')+'?'+new URLSearchParams({q:assetQuery,offset:String(assetOffset)}),{credentials:'same-origin'}).then(async r=>{if(!r.ok)throw Error();return r.json()}).then(r=>{if(!cancel){setAssets(r.items||[]);setAssetNext(!!r.next)}}).catch(()=>{if(!cancel)setAssetError('Не удалось загрузить медиатеку')})}return()=>{cancel=true}},[stage,assetQuery,assetOffset]);
 const change=(field,v)=>onChange({...value,[field]:v});
 const taskChange=(index,field,v)=>change(key,tasks.map((t,i)=>i===index?{...t,[field]:v}:t));
 const move=(index,direction)=>{const n=[...tasks],to=index+direction;if(to<0||to>=n.length)return;[n[index],n[to]]=[n[to],n[index]];change(key,n)};
 return <div className="task-editor">
 <div className="editor-meta"><label>Название группы<input disabled={disabled} value={value.name||''} onChange={e=>change('name',e.target.value)}/></label><label>Доступ<select disabled={disabled} value={value.target||'VIP+FREE'} onChange={e=>change('target',e.target.value)}>{['FREE','VIP','VIP+FREE'].map(x=><option key={x} value={x}>{({FREE:'Бесплатный',VIP:'VIP','VIP+FREE':'Для всех'}[x])}</option>)}</select></label></div>
 <label>Описание<input disabled={disabled} value={value.description||''} onChange={e=>change('description',e.target.value)}/></label>
 <div className="stage-tabs" role="tablist" aria-label="Этапы урока">{definitions.map(([label,k],i)=><button type="button" key={k} role="tab" aria-selected={stage===i} className={stage===i?'selected':''} onClick={()=>{setStage(i);setAssetQuery('');setAssetOffset(0)}}><span>{i+1}</span>{label}<small>{(value[k]||[]).length}</small></button>)}</div>
 <div className="stage-heading"><h3>{stage+1}. {name}</h3><button disabled={disabled||(stage===7&&tasks.length>0)} onClick={()=>change(key,[...tasks,{...structuredClone(defaults[key]),task_id:null}])}><Plus size={15}/>Добавить задание</button></div>
 {(stage===4||stage===6)&&<div className="asset-picker-toolbar"><label>Поиск медиа по ID<input value={assetQuery} onChange={e=>{setAssetQuery(e.target.value);setAssetOffset(0)}} placeholder="SHA-256 или его часть"/></label><div className="actions"><button disabled={assetOffset===0} onClick={()=>setAssetOffset(x=>Math.max(0,x-50))}>Предыдущие файлы</button><button disabled={!assetNext} onClick={()=>setAssetOffset(x=>x+50)}>Следующие файлы</button></div></div>}
 {tasks.length===0&&<p className="empty">На этом этапе пока нет заданий.</p>}
 {tasks.map((task,i)=><section className="task-card" key={i}><header><b>Задание {i+1}</b><small>{task.task_id?'Постоянный ID '+task.task_id:'Новое задание'}</small><div className="task-buttons"><button disabled={disabled||i===0} aria-label={'Переместить задание '+(i+1)+' вверх'} onClick={()=>move(i,-1)}><ArrowUp size={15}/></button><button disabled={disabled||i===tasks.length-1} aria-label={'Переместить задание '+(i+1)+' вниз'} onClick={()=>move(i,1)}><ArrowDown size={15}/></button><button disabled={disabled} aria-label={'Удалить задание '+(i+1)} onClick={()=>{if(window.confirm('Удалить задание из черновика? История опубликованных версий сохранится.'))change(key,tasks.filter((_,j)=>i!==j))}}><Trash2 size={15}/></button></div></header>
 <div className="task-fields">{fields.map(field=><label key={field}>{fieldLabels[field]||field}{arrays.has(field)?<textarea disabled={disabled} rows={3} placeholder="Одно значение на строку" value={(task[field]||[]).join('\n')} onChange={e=>taskChange(i,field,e.target.value.split('\n'))}/>:field==='text'?<textarea disabled={disabled} value={task[field]||''} onChange={e=>taskChange(i,field,e.target.value)}/>:<input disabled={disabled} value={task[field]||''} onChange={e=>taskChange(i,field,e.target.value)}/>}
 {(field==='image'||field==='audio')&&<><select aria-label={'Выбрать файл: '+fieldLabels[field]+', задание '+(i+1)} disabled={disabled} value={assets.some(a=>a.filename===task[field])?task[field]:''} onChange={e=>{if(e.target.value)taskChange(i,field,e.target.value)}}><option value="">Выберите файл: {fieldLabels[field]}</option>{assets.map(a=><option key={a.asset_id} value={a.filename}>{a.filename.slice(0,24)}…</option>)}</select><button type="button" disabled={disabled||task[field]==='__detached__.'+(field==='image'?'jpg':'mp3')} onClick={()=>{if(window.confirm('Отвязать файл от черновика? Перед публикацией его нужно заменить.'))taskChange(i,field,'__detached__.'+(field==='image'?'jpg':'mp3'))}}>Отвязать файл</button>{assetError&&<small>{assetError}</small>}{assets.some(a=>a.filename===task[field])&&(field==='image'?<img className="task-preview" src={'/api/asset?kind=image&id='+assets.find(a=>a.filename===task[field]).asset_id} alt="Выбранный файл"/>:<audio controls src={'/api/asset?kind=audio&id='+assets.find(a=>a.filename===task[field]).asset_id}/>)}</>}
 </label>)}</div></section>)}
 </div>
}
