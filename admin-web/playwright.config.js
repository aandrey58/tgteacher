import {defineConfig} from '@playwright/test';

const widths=[{width:1536,height:960},{width:320,height:568},{width:375,height:812},{width:390,height:844},{width:430,height:932},{width:768,height:1024}];
const matrix=[...widths.map(viewport=>({browserName:'chromium',viewport})),{browserName:'webkit',viewport:widths[3]}];

export default defineConfig({
 testDir:'./tests',
 workers:1,
 timeout:90000,
 projects:matrix.map(({browserName,viewport})=>({
  name:browserName+'-'+viewport.width,
  use:{browserName,viewport,isMobile:viewport.width<=900,hasTouch:viewport.width<=900}
 }))
});
