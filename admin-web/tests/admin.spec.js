import {test,expect} from '@playwright/test';

test('authenticated admin',async({page,browserName},testInfo)=>{
  const viewport=page.viewportSize();
  test.setTimeout(150000);
  const browserErrors=[];
  page.on('pageerror',e=>browserErrors.push(e.message));
  const groupName='Browser '+browserName+' '+viewport.width;
  const compact=viewport.width<=900;
  const noOverflow=async()=>expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth+1)).toBeTruthy();
  await page.goto('http://127.0.0.1:8080/');
  await expect(page.locator('html')).toHaveAttribute('lang','ru');
  await page.getByLabel('Ключ администратора').fill('ci-only-web-admin-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
  await page.getByRole('button',{name:'Войти'}).click();
  await expect(page.getByRole('heading',{name:'Главная',exact:true,level:1})).toBeVisible();
  await expect(page.getByText('Активность обучения',{exact:true})).toBeVisible();
  await noOverflow();
  if(compact){
    const menu=page.getByRole('button',{name:'Открыть меню'});
    expect(await menu.evaluate(el=>el.getBoundingClientRect().width)).toBeGreaterThanOrEqual(44);
    await menu.tap();
    const drawer=page.getByRole('dialog',{name:'Навигация'});
    await expect(drawer).toBeVisible();
    await expect(drawer.getByRole('button',{name:'Закрыть'})).toBeFocused();
    await expect(drawer.locator('[aria-current="page"]')).toHaveText('Главная');
    await page.screenshot({path:testInfo.outputPath('navigation.png')});
    await page.keyboard.press('Escape');
    await expect(drawer).toHaveCount(0);
    await expect(menu).toBeFocused();
    await expect(menu).toHaveAttribute('aria-expanded','false');
  }
  await page.screenshot({path:testInfo.outputPath('dashboard.png'),fullPage:true});
  for(const [nav,title] of [['Группы','Группы слов'],['Черновики','Черновики'],['Ученики','Ученики'],['Повторения','Повторения'],['Медиатека','Медиатека'],['Озвучка','Озвучка'],['Изображения','Изображения'],['Бюджеты','Бюджеты'],['Настройки','Настройки']]){
    if(compact)await page.getByRole('button',{name:'Открыть меню'}).click();
    await page.locator('nav').getByRole('button',{name:nav,exact:true}).click();
    await expect(page.getByRole('heading',{name:title,exact:true,level:1})).toBeVisible();
    await expect(page.getByText('Загрузка…',{exact:true})).toHaveCount(0);
    await expect(page.getByRole('alert')).toHaveCount(0);
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth+1)).toBeTruthy();
    if(nav==='Группы') {
      await expect(page.getByText('Travel essentials',{exact:true}).first()).toBeVisible();
      const search=page.getByPlaceholder('Поиск групп слов…');
      await search.pressSequentially('passport',{delay:75});
      await expect(search).toBeFocused();
      await expect(search).toHaveValue('passport');
      await expect(page.getByRole('cell').filter({hasText:'Travel essentials'})).toBeVisible();
      await expect(page.getByRole('cell').filter({hasText:'Food & cooking'})).toHaveCount(0);
      await search.clear();
      await expect(page.getByRole('cell').filter({hasText:'Food & cooking'})).toBeVisible();
    }
    if(nav==='Медиатека') await expect.poll(()=>page.locator('.asset-card').count()).toBeGreaterThanOrEqual(3);
    await page.screenshot({path:testInfo.outputPath(nav.toLowerCase()+'.png'),fullPage:true});
  }
  if(compact)await page.getByRole('button',{name:'Открыть меню'}).click();
  await page.locator('nav').getByRole('button',{name:'Группы',exact:true}).click();
  await page.getByRole('button',{name:'Создать группу'}).click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await expect(page.getByRole('button',{name:'Закрыть',exact:true})).toBeFocused();
  await expect.poll(()=>page.evaluate(()=>document.body.style.overflow)).toBe('hidden');
  await page.getByLabel('Исходный текст урока (TGTeacher TXT)').fill('#FAMILY "'+groupName+'"\n#WORD ticket\n#TRANSLATION билет\n');
  await page.getByRole('button',{name:'Создать черновик',exact:true}).click();
  await expect(page.getByText('Редакция 1',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Визуальный редактор'}).click();
  const tabNames=['Слова','Заполнение пропусков','Определения','Слова в контексте','Слова по картинке','Синонимы','Аудирование','Итоговый текст'];
  const tablist=page.getByRole('tablist',{name:'Этапы урока'});
  await expect(tablist.getByRole('tab')).toHaveCount(8);
  for(const name of tabNames){
    const tab=tablist.getByRole('tab',{name,exact:true});
    await expect(tab).toHaveCount(1);
    await tab.click();
    await expect(tab).toHaveAttribute('aria-selected','true');
    await expect(tablist.locator('[aria-selected="true"]')).toHaveCount(1);
  }
  await tablist.getByRole('tab',{name:'Слова',exact:true}).click();
  await expect(tablist.getByRole('tab',{name:'Слова',exact:true})).toHaveAttribute('aria-selected','true');
  await page.getByLabel('Перевод',{exact:true}).fill('проездной билет');
  // Escape must not discard unsaved work if the operator rejects confirmation.
  page.once('dialog',d=>d.dismiss());
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog')).toBeVisible();
  await expect(page.getByLabel('Перевод',{exact:true})).toHaveValue('проездной билет');
  await noOverflow();
  expect(await page.locator('dialog.modal-shell').evaluate(el=>el.scrollWidth<=el.clientWidth+1)).toBeTruthy();
  await page.getByRole('button',{name:'Сохранить редакцию'}).click();
  await expect(page.getByText('Редакция 2',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Проверить редакцию'}).click();
  await expect(page.getByRole('heading',{name:'Сохранённая редакция прошла проверку'})).toBeVisible();
  await expect(page.getByLabel('Перевод',{exact:true})).toHaveValue('проездной билет');
  await noOverflow();
  if(compact){
    const box=await page.getByRole('button',{name:'Сохранить редакцию'}).boundingBox();
    expect(box.height).toBeGreaterThanOrEqual(44);
  }
  await page.screenshot({path:testInfo.outputPath('editor.png'),fullPage:true});
  page.once('dialog',d=>d.accept());
  await page.getByRole('button',{name:'Опубликовать',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Группы слов',exact:true,level:1})).toBeVisible();
  await page.getByLabel('Фильтр статуса').selectOption('Published');
  await page.getByPlaceholder('Поиск групп слов…').fill(groupName);
  await expect(page.getByRole('cell').filter({hasText:groupName}).first()).toBeVisible();
  await page.getByRole('row').filter({has:page.getByRole('cell').filter({hasText:groupName})}).getByRole('button',{name:'Открыть',exact:true}).click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByRole('button',{name:'История версий'}).click();
  await page.getByRole('button',{name:'Сравнить',exact:true}).first().click();
  await expect(page.getByRole('heading',{name:'Версия 1 → 1'})).toBeVisible();
  await page.getByText('Текущий материал урока',{exact:true}).click();
  await expect(page.getByLabel('Перевод',{exact:true})).toHaveValue('проездной билет');
  let downloaded;
  const operations=browserName==='chromium'&&[1536,390].includes(viewport.width);
  if(operations){
    const event=page.waitForEvent('download');
    await page.getByRole('link',{name:'Скачать ZIP'}).click();
    downloaded=await event;
    expect(downloaded.suggestedFilename()).toMatch(/\.zip$/);
  }
  await page.getByRole('button',{name:'Закрыть',exact:true}).click();
  if(operations){
    await page.getByRole('checkbox',{name:'Выбрать '+groupName,exact:true}).check();
    page.once('dialog',d=>d.accept());
    await page.getByRole('button',{name:'Архивировать выбранные'}).click();
    await expect(page.getByRole('cell').filter({hasText:groupName})).toHaveCount(0);
    await page.getByLabel('Фильтр статуса').selectOption('Archived');
    await page.getByRole('checkbox',{name:'Выбрать '+groupName,exact:true}).check();
    page.once('dialog',d=>d.accept());
    await page.getByRole('button',{name:'Восстановить выбранные'}).click();
    await page.getByLabel('Фильтр статуса').selectOption('Published');
    await expect(page.getByRole('cell').filter({hasText:groupName}).first()).toBeVisible();
    await page.getByText('Импорт и шаблон урока',{exact:true}).click();
    await page.getByLabel('ZIP-архив урока').setInputFiles(await downloaded.path());
    page.once('dialog',d=>d.accept());
    await page.getByRole('button',{name:'Импортировать ZIP'}).click();
    await expect(page.getByText('Редакция 1',{exact:true})).toBeVisible();
    await page.getByRole('button',{name:'Проверить редакцию'}).click();
    // Same name already exists; import must never silently publish.
    await expect(page.getByRole('heading',{name:'В сохранённой редакции есть ошибки'})).toBeVisible();
    await page.screenshot({path:testInfo.outputPath('zip-import.png'),fullPage:true});
    await page.getByRole('button',{name:'Закрыть',exact:true}).click();
    if(compact)await page.getByRole('button',{name:'Открыть меню'}).click();
    await page.locator('nav').getByRole('button',{name:'Ученики',exact:true}).click();
    await page.getByRole('button',{name:'Рассылки и обращения'}).click();
    const dialog=page.getByRole('dialog',{name:'Рассылки и обращения'});
    await expect(dialog.getByRole('heading',{name:'Новое сообщение'})).toBeVisible();
    await dialog.getByLabel('Текст сообщения').fill('Тест CI '+viewport.width);
    await dialog.getByText('Прикрепить файл',{exact:true}).click();
    await dialog.getByLabel('Файл',{exact:true}).setInputFiles({name:'ci-guide.txt',mimeType:'text/plain',buffer:Buffer.from('CI fixture')});
    await dialog.getByRole('button',{name:'Загрузить вложение'}).click();
    await expect(dialog.getByText('ci-guide.txt',{exact:false})).toBeVisible();
    await dialog.getByRole('button',{name:'Подготовить рассылку'}).click();
    await expect(dialog.getByRole('heading',{name:'Подтверждение рассылки'})).toBeVisible();
    await page.screenshot({path:testInfo.outputPath('campaign-preview.png'),fullPage:true});
    page.once('dialog',d=>d.accept());
    await dialog.getByRole('button',{name:'Отменить черновик'}).click();
    await expect(dialog.getByRole('heading',{name:'Подтверждение рассылки'})).toHaveCount(0);
    await dialog.getByRole('button',{name:'Обращения',exact:true}).click();
    await expect(dialog.getByRole('heading',{name:'Обращения учеников'})).toBeVisible();
    await dialog.getByRole('button',{name:'Закрыть',exact:true}).click();
    await page.getByRole('button',{name:'Подписки и отчёты'}).click();
    const manage=page.getByRole('dialog',{name:'Подписки и отчёты'});
    await manage.getByRole('button',{name:'Отчёты',exact:true}).click();
    await expect(manage.getByRole('heading',{name:'Выгрузка Excel'})).toBeVisible();
    const reportEvent=page.waitForEvent('download');
    await manage.getByRole('link',{name:'Скачать Excel'}).click();
    expect((await reportEvent).suggestedFilename()).toBe('report.xlsx');
    await page.screenshot({path:testInfo.outputPath('reports.png'),fullPage:true});
    await manage.getByRole('button',{name:'Закрыть',exact:true}).click();
    await noOverflow();
  }
  await expect.poll(()=>page.evaluate(()=>document.body.style.overflow)).toBe('');
  await page.getByRole('button',{name:'Выйти'}).click();
  await expect(page.getByRole('button',{name:'Войти'})).toBeVisible();
  expect(browserErrors).toEqual([]);
});

test('personal accounts and role boundaries',async({page,browserName},testInfo)=>{
  const viewport=page.viewportSize();
  test.skip(!((browserName==='chromium'&&[1536,320].includes(viewport.width))||(browserName==='webkit'&&viewport.width===390)),'representative desktop, minimum phone and WebKit');
  const root='http://127.0.0.1:8080';
  const origin={Origin:root};
  const suffix=browserName+'-'+viewport.width+'-'+Date.now();
  const password='ci-role-password-12345';
  const action=(name,args={},confirm=false)=>page.request.post(root+'/api/action',{headers:origin,data:{action:name,args,confirm}});
  const signIn=async(username,secret)=>{
    await page.getByLabel('Логин',{exact:true}).fill(username);
    await page.getByLabel(username?'Пароль':'Ключ администратора',{exact:true}).fill(secret);
    await page.getByRole('button',{name:'Войти',exact:true}).click();
  };
  await page.goto(root);
  await signIn('','ci-only-web-admin-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
  await expect(page.getByRole('heading',{name:'Главная',level:1,exact:true})).toBeVisible();
  for(const role of ['editor','support']){
    const res=await action('create_account',{username:role+'-'+suffix,password,role},true);
    expect(res.status()).toBe(200);
  }
  const broadcast=await action('create_campaign',{campaign_id:crypto.randomUUID(),target:'all',text:'PRIVATE CI BROADCAST '+suffix},true);
  expect(broadcast.status()).toBe(200);
  const campaign=(await broadcast.json()).id;
  const question=(await (await action('support')).json()).items.find(x=>x.question==='CI role test question');
  expect(question).toBeTruthy();
  const prepared=await action('create_campaign',{campaign_id:crypto.randomUUID(),target:'support',question_id:question.id,text:'CI DRAFT '+suffix},true);
  expect(prepared.status()).toBe(200);
  await page.getByRole('button',{name:'Выйти',exact:true}).click();
  await signIn('editor-'+suffix,password);
  await expect(page.getByRole('heading',{name:'Главная',level:1,exact:true})).toBeVisible();
  if(viewport.width<=900)await page.getByRole('button',{name:'Открыть меню'}).click();
  await expect(page.locator('nav').getByRole('button',{name:'Группы',exact:true})).toBeVisible();
  await expect(page.locator('nav').getByRole('button',{name:'Ученики',exact:true})).toHaveCount(0);
  await expect(page.locator('nav').getByRole('button',{name:'Настройки',exact:true})).toHaveCount(0);
  expect((await page.request.get(root+'/api/learners')).status()).toBe(403);
  if(viewport.width<=900)await page.keyboard.press('Escape');
  await page.getByRole('button',{name:'Выйти',exact:true}).click();
  await signIn('support-'+suffix,password);
  await expect(page.getByRole('heading',{name:'Обращения учеников',level:1})).toBeVisible();
  await expect(page.getByText('CI role test question',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'История ответов',exact:true}).click();
  const draft=page.locator('article').filter({hasText:'CI DRAFT '+suffix});
  await expect(draft).toBeVisible();
  await expect(page.getByText('PRIVATE CI BROADCAST '+suffix,{exact:true})).toHaveCount(0);
  await draft.getByRole('button',{name:'Открыть подтверждение'}).click();
  await expect(page.getByRole('heading',{name:'Подтверждение ответа'})).toBeVisible();
  await page.screenshot({path:testInfo.outputPath('support-role.png'),fullPage:true});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth+1)).toBeTruthy();
  expect((await action('confirm_campaign',{campaign_id:campaign,expected_count:1},true)).status()).toBe(403);
  page.once('dialog',d=>d.accept());
  await page.getByRole('button',{name:'Отменить черновик'}).click();
  await expect(page.getByRole('heading',{name:'Подтверждение ответа'})).toHaveCount(0);
  await page.getByRole('button',{name:'Выйти',exact:true}).click();
  await expect(page.getByRole('button',{name:'Войти',exact:true})).toBeVisible();
});
