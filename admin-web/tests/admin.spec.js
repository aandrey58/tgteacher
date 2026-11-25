import {test,expect} from '@playwright/test';

for (const viewport of [{width:1536,height:960},{width:390,height:844}]) {
 test('authenticated admin at '+viewport.width,async({page},testInfo)=>{
  await page.setViewportSize(viewport);
  await page.goto('http://127.0.0.1:8080/');
  await page.getByLabel('Administrator credential').fill('ci-only-web-admin-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
  await page.getByRole('button',{name:'Sign in'}).click();
  await expect(page.getByRole('heading',{name:'Dashboard',exact:true,level:1})).toBeVisible();
  await expect(page.getByText('Learning activity',{exact:true})).toBeVisible();
  await page.screenshot({path:testInfo.outputPath('dashboard.png'),fullPage:true});
  for(const [nav,title] of [['Groups','Word Groups'],['Drafts','Drafts'],['Learners','Learners'],['Reviews','Reviews'],['Media','Media'],['TTS','TTS'],['Images','Images'],['Budgets','Budgets'],['Settings','Settings']]){
    if(viewport.width<760)await page.getByRole('button',{name:'Open menu'}).click();
    await page.locator('nav').getByRole('button',{name:nav,exact:true}).click();
    await expect(page.getByRole('heading',{name:title,exact:true,level:1})).toBeVisible();
    await expect(page.getByText('Loading…',{exact:true})).toHaveCount(0);
    await expect(page.getByRole('alert')).toHaveCount(0);
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth+1)).toBeTruthy();
    await page.screenshot({path:testInfo.outputPath(nav.toLowerCase()+'.png'),fullPage:true});
  }
  if(viewport.width<760)await page.getByRole('button',{name:'Open menu'}).click();
  await page.locator('nav').getByRole('button',{name:'Groups',exact:true}).click();
  await page.getByRole('button',{name:'Create group'}).click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByLabel('Lesson source (TGTeacher TXT)').fill('#FAMILY "Browser '+viewport.width+'"\n#WORD ticket\n#TRANSLATION билет\n');
  await page.getByRole('button',{name:'Create draft',exact:true}).click();
  await expect(page.getByText('Revision 1',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Visual editor'}).click();
  await expect(page.getByRole('tab',{name:/Words/})).toBeVisible();
  await page.getByLabel('translation',{exact:true}).fill('проездной билет');
  await page.getByRole('button',{name:'Save revision'}).click();
  await expect(page.getByText('Revision 2',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Validate saved revision'}).click();
  await expect(page.getByRole('heading',{name:'Saved revision passed validation'})).toBeVisible();
  await page.screenshot({path:testInfo.outputPath('editor.png'),fullPage:true});
  page.once('dialog',d=>d.accept());
  await page.getByRole('button',{name:'Publish',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Word Groups',exact:true,level:1})).toBeVisible();
  await page.getByLabel('Filter status').selectOption('Published');
  await page.getByPlaceholder('Search word groups…').fill('Browser '+viewport.width);
  await expect(page.getByRole('cell').filter({hasText:'Browser '+viewport.width}).first()).toBeVisible();
  await page.getByRole('button',{name:'Open',exact:false}).filter({hasText:'Open'}).first().click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByRole('button',{name:'Version history'}).click();
  await page.getByRole('button',{name:'Compare',exact:true}).first().click();
  await expect(page.getByRole('heading',{name:'Version 1 → 1'})).toBeVisible();
  await page.getByRole('button',{name:'Close',exact:true}).click();
  await page.getByRole('button',{name:'Sign out'}).click();
  await expect(page.getByRole('button',{name:'Sign in'})).toBeVisible();
 });
}
