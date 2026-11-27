import React,{useEffect,useRef,useState} from 'react';

// Native modal dialogs provide a top layer, focus containment and inert background.
export function useCompactLayout(){
 const [compact,setCompact]=useState(()=>window.matchMedia('(max-width: 900px)').matches);
 useEffect(()=>{
  const mq=window.matchMedia('(max-width: 900px)');
  const sync=()=>setCompact(mq.matches);
  mq.addEventListener('change',sync);sync();
  return ()=>mq.removeEventListener('change',sync);
 },[]);
 return compact;
}
export default function Overlay({title,close,children,drawer=false}){
 const ref=useRef(null),closeRef=useRef(close);
 closeRef.current=close;
 useEffect(()=>{
  const el=ref.current,previous=document.activeElement;
  const oldOverflow=document.body.style.overflow;
  document.body.style.overflow='hidden';
  el.showModal();
  el.querySelector('[data-initial-focus]')?.focus({preventScroll:true});
  return ()=>{
   if(el.open)el.close();
   document.body.style.overflow=oldOverflow;
   if(previous?.isConnected)previous.focus({preventScroll:true});
  };
 },[]);
 return <dialog ref={ref} className={drawer?'drawer-dialog':'modal-shell'} aria-label={title}
  onCancel={e=>{e.preventDefault();closeRef.current()}}
  onClick={e=>{if(e.target===e.currentTarget){const rect=e.currentTarget.getBoundingClientRect();if(e.clientX<rect.left||e.clientX>rect.right||e.clientY<rect.top||e.clientY>rect.bottom)closeRef.current()}}}>
  <section className={drawer?'drawer-body':'dialog'}>
   <header className="overlay-header"><h2>{title}</h2><button type="button" className="icon overlay-close" data-initial-focus aria-label="Закрыть" onClick={()=>closeRef.current()}>×</button></header>
   {children}
  </section>
 </dialog>;
}
