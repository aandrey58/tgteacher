import {test,expect} from '@playwright/test';

test('authenticated admin',async({page,browserName},testInfo)=>{
  const viewport=page.viewportSize();
  test.setTimeout(90000);
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
    if(nav==='Медиатека') await expect(page.locator('.asset-card')).toHaveCount(3);
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
  await page.getByRole('button',{name:'Закрыть',exact:true}).click();
  await expect.poll(()=>page.evaluate(()=>document.body.style.overflow)).toBe('');
  await page.getByRole('button',{name:'Выйти'}).click();
  await expect(page.getByRole('button',{name:'Войти'})).toBeVisible();
  expect(browserErrors).toEqual([]);
});
