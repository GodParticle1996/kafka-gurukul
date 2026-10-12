#!/usr/bin/env node
/* Boot test — app.js ke saare screens bina DOM ke render karke dekho */
const fs=require('fs'), path=require('path'), vm=require('vm');
const DIR=__dirname;
const noop=()=>{};
function mkEl(tag){
  const e={ tagName:tag, className:'', innerHTML:'', textContent:'', value:'', disabled:false,
    style:{}, dataset:{}, children:[], _l:{},
    classList:{ add:noop, remove:noop, contains:()=>false, toggle:noop },
    appendChild(c){ this.children.push(c); return c; },
    append(...cs){ cs.forEach(c=>this.children.push(c)); },
    insertBefore(c){ this.children.unshift(c); return c; },
    removeChild(c){ return c; }, remove:noop,
    addEventListener(k,f){ this._l[k]=f; }, removeEventListener:noop,
    querySelector(){ return mkEl('div'); }, querySelectorAll(){ return []; },
    scrollIntoView:noop, focus:noop, click(){ this.onclick&&this.onclick(); },
    setAttribute:noop, getAttribute:()=>null, closest:()=>null,
    getBoundingClientRect:()=>({top:0,left:0,width:100,height:100}),
  };
  return e;
}
const sandbox={ console,Math,Date,JSON,Object,Array,String,Number,Boolean,Error,Map,Set,Proxy,Symbol,
  RegExp,isNaN,parseInt,parseFloat,setTimeout:noop,clearTimeout:noop,setInterval:noop,
  module:{exports:{}}, confirm:()=>false, alert:noop,
  localStorage:{ _d:{}, getItem(k){return this._d[k]||null;}, setItem(k,v){this._d[k]=v;}, removeItem(k){delete this._d[k];} },
  document:{ getElementById:()=>mkEl('div'), createElement:mkEl, querySelector:()=>mkEl('div'),
    querySelectorAll:()=>[], addEventListener:noop, body:mkEl('body') },
  window:{ scrollTo:noop, addEventListener:noop },
  Element:{ prototype:{} },
};
sandbox.globalThis=sandbox; vm.createContext(sandbox);
const FILES=['core.js','data-core.js','visuals.js','games.js','kafkacli.js','lab.js',
  'data-beginner.js','data-intermediate.js','data-advanced.js','data-sysdesign.js','app.js'];
const src=FILES.map(f=>`/* ${f} */\n`+fs.readFileSync(path.join(DIR,f),'utf8')).join('\n')
  +`\nglobalThis.__KQ=KQ; globalThis.__fns={showHome,showTrack,renderTopbar,
     showLevelIntro:typeof showLevelIntro!=='undefined'?showLevelIntro:null,
     showDojo:typeof showDojo!=='undefined'?showDojo:null};`;
let pass=0,fail=0;
const ok=(n,f)=>{ try{ f(); pass++; console.log('  ✅',n); }catch(e){ fail++; console.log('  ❌',n,'\n     →',e.message); } };
try{ vm.runInContext(src,sandbox,{filename:'bundle.js'}); }
catch(e){ console.log('❌ BOOT FAILED:',e.message,'\n',e.stack.split('\n').slice(0,5).join('\n')); process.exit(1); }
console.log('=== Boot & screens ===');
const KQ=sandbox.__KQ, F=sandbox.__fns;
ok('app boot hua (top-level crash nahi)',()=>{ if(!KQ) throw new Error('KQ nahi mila'); });
ok('topbar render hua',()=>F.renderTopbar());
ok('home screen render hua',()=>F.showHome());
ok('har track ka map render hua',()=>KQ.tracks.forEach(t=>F.showTrack(t)));
ok('har level ka intro render hua',()=>{
  KQ.tracks.forEach(t=>(KQ.levelSets[t.id]||[]).forEach(l=>{
    if(F.showLevelIntro) F.showLevelIntro(l);
  }));
});
ok('Revision Dojo render hua',()=>{ if(F.showDojo) F.showDojo(); });
ok('har level ka diagram HTML valid-ish hai',()=>{
  const V=vm.runInContext('typeof VISUALS!=="undefined"?VISUALS:{}',sandbox);
  Object.entries(V).forEach(([k,v])=>{
    if(!v.t||!v.html) throw new Error(k+' mein t/html missing');
    const o=(v.html.match(/<div/g)||[]).length, c=(v.html.match(/<\/div>/g)||[]).length;
    if(o!==c) throw new Error(`${k}: <div> ${o} vs </div> ${c} — tag balance galat`);
  });
});
ok('XP/rank engine chalta hai',()=>{
  KQ.state.xp=0; if(KQ.currentRank().name!=='Sandesh Shishya') throw new Error('rank 0 galat');
  KQ.state.xp=5000; if(KQ.currentRank().name!=='Kafka Mahaguru') throw new Error('max rank galat');
  KQ.state.xp=0;
});
ok('SRS dojo engine chalta hai',()=>{
  KQ.addCard('t1','front','back','tag');
  if(!KQ.state.dojo['t1']) throw new Error('card add nahi hua');
  KQ.state.dojo['t1'].due=Date.now()-1000;
  if(!KQ.dueCards().some(c=>c.id==='t1')) throw new Error('due card nahi mila');
  KQ.reviewCard('t1',true);
  if(KQ.state.dojo['t1'].box!==1) throw new Error('box aage nahi badha');
});
console.log(`\n  ${pass} pass · ${fail} fail`);
process.exit(fail?1:0);
