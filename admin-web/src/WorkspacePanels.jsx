import React from 'react';
import {Clock,CheckCircle2,RotateCcw,Layers,FileText,Users,CalendarCheck,Image,AudioLines,Database,ShieldCheck,ArrowRight,BookOpen,PlayCircle} from 'lucide-react';

export function formatDate(value){
 if(!value)return '—';
 const d=new Date(value);
 return Number.isNaN(d.getTime())?String(value):d.toLocaleDateString('ru-RU',{day:'2-digit',month:'short',year:'numeric'});
}
const metrics={'MCP tools':[Database,'Инструменты MCP'],'Web access':[ShieldCheck,'Веб-доступ'],'Database':[Database,'База данных'],
 published:[Layers,'Опубликованные группы'],published_groups:[Layers,'Опубликованные группы'],archived:[Layers,'Архивные группы'],archived_groups:[Layers,'Архивные группы'],
 open_drafts:[FileText,'Открытые черновики'],drafts:[FileText,'Черновики'],edit_drafts:[FileText,'Черновики правок'],published_drafts:[CheckCircle2,'Опубликованные черновики'],
 active_learners:[Users,'Активные за 7 дней'],total_learners:[Users,'Всего учеников'],new_learners:[Users,'Новые за 7 дней'],vip_learners:[ShieldCheck,'Ученики VIP'],
 words:[BookOpen,'Активные слова'],versions:[Clock,'Версии материалов'],due_cards:[CalendarCheck,'К повторению'],active_cards:[CalendarCheck,'Активные карточки'],rated_cards:[CheckCircle2,'Оценённые карточки'],reviewed_today:[CheckCircle2,'Повторения сегодня · UTC'],
 audio_assets:[AudioLines,'Аудиофайлы'],image_assets:[Image,'Изображения'],historical_audio_links:[AudioLines,'Аудио в версиях'],historical_image_links:[Image,'Изображения в версиях'],
 ready_requests:[CheckCircle2,'Готовые запросы'],pending_requests:[Clock,'Ожидающие запросы'],failed_requests:[RotateCcw,'Ошибки запросов'],TTS:[AudioLines,'Резерв озвучки сегодня'],Images:[Image,'Резерв изображений сегодня']
};
export function MetricCards({data,page}){
 if(!data)return null;
 return <div className="metrics">{Object.entries(data).map(([key,v],i)=>{const [Icon,label]=metrics[key]||[Database,key.replaceAll('_',' ').replace(/^./,x=>x.toUpperCase())];return <section key={key} className={'metric tint'+i%4}><div className="metric-label"><Icon size={21}/>{label}</div><strong>{page==='budgets'?'$'+(v/1e6).toFixed(4):typeof v==='number'?v.toLocaleString('ru-RU'):({'Single operator':'Один оператор',Connected:'Подключена'}[v]??v)}</strong><small>{page==='budgets'?'Резерв, не счёт':'Данные базы'}</small></section>})}</div>
}
export function PublishingActivity({items=[],open,go}){
 return <section className="panel publishing"><h2><Clock size={20}/>Последние публикации</h2>{items.length?<div className="timeline">{items.map(v=><button className="timeline-item" key={v.family_id+'-'+v.version} onClick={()=>open({id:v.family_id,name:v.name})}><span className="timeline-icon">{v.restored_from?<RotateCcw size={16}/>:<CheckCircle2 size={17}/>}</span><span><b>{v.name}</b><small>Версия {v.version}{v.restored_from?' · восстановлена v'+v.restored_from:' · опубликована'}</small><small>{formatDate(v.created_at)}</small></span><ArrowRight size={14}/></button>)}</div>:<p className="empty">Публикаций пока нет.</p>}<button className="link-button" onClick={()=>go('groups')}>Все группы <ArrowRight size={15}/></button></section>
}
const railCopy={drafts:['Работа с черновиками','Сохраните редакцию, проверьте материал и опубликуйте его. История обучения сохраняется.'],learners:['Доступ учеников','VIP определяется действующей подпиской. Активность отражает реальные действия, а не прогноз запоминания.'],reviews:['Нагрузка FSRS','Карточки к повторению подсчитаны по текущему расписанию. Число оценённых карточек не отражает точность запоминания.'],media:['История медиа','Сгенерированные файлы доступны прошлым версиям. Откройте файл, чтобы увидеть происхождение и использование.'],tts:['Генерация озвучки','Выберите модель и голос, оцените стоимость и подтвердите генерацию. Кэшированные результаты не требуют повторного платного запроса.'],images:['Генерация изображений','Изображения преобразуются в JPEG и хранятся по хешу содержимого. Выберите настроенную модель.'],budgets:['Правила бюджета','Лимиты обновляются в полночь UTC. Неудачные и ожидающие запросы сохраняют резерв во избежание двойной оплаты.'],settings:['Безопасность','Веб-сессия не зависит от ключей MCP и учеников. Секреты остаются на сервере.']};
export function WorkspaceRail({page,data,open,go}){
 const copy=railCopy[page]||['Управление материалами','Создавайте группы из восьми этапов. Публикуйте проверенные черновики с сохранением предыдущих версий.'];
 return <aside className="right-rail">{page==='groups'?<PublishingActivity items={data.publishing} open={open} go={go}/>:<section className="panel"><h2><ShieldCheck size={20}/>{copy[0]}</h2><p className="muted">{copy[1]}</p></section>}<section className="panel accent"><div className="rail-art"><BookOpen size={33}/><span><Layers size={27}/></span></div><h2>{page==='groups'?'Создавайте группы слов':'Рабочее пространство TGTeacher'}</h2><p>{page==='groups'?'Создавайте, проверяйте и публикуйте материалы по английскому.':'Изменения сохраняются в общей базе PostgreSQL.'}</p><small><CheckCircle2 size={14}/> История версий</small><small><CheckCircle2 size={14}/> Сохранение прогресса учеников</small>{page==='groups'&&<button className="link-button" onClick={()=>go('drafts')}>Открыть черновики <ArrowRight size={15}/></button>}</section></aside>
}
export function AssetGallery({rows,open}){
 return <div className="asset-gallery">{rows.map(r=><button className="asset-card" key={r.kind+r.asset_id} onClick={()=>open(r)}><div className={'asset-visual '+r.kind}>{r.kind==='image'?<img loading="lazy" alt="Сгенерированное изображение" src={'/api/asset?kind=image&id='+encodeURIComponent(r.asset_id)}/>:<><AudioLines size={48}/><PlayCircle size={25}/></>}</div><div className="asset-info"><b>{r.filename.slice(0,20)}…</b><span>{r.kind==='image'?r.width+' × '+r.height:'MP3'} · {Math.ceil(r.size_bytes/1024)} КБ</span><small>{formatDate(r.created_at)}</small><span className="asset-link">Открыть файл <ArrowRight size={14}/></span></div></button>)}</div>
}
