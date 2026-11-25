import {test,expect} from '@playwright/test';

for (const viewport of [{width:1536,height:960},{width:390,height:844}]) {
 test('authenticated admin at '+viewport.width,async({page},testInfo)=>{
  test.setTimeout(90000);
  const browserErrors=[];
  page.on('pageerror',e=>browserErrors.push(e.message));
  await page.setViewportSize(viewport);
  await page.goto('http://127.0.0.1:8080/');
  await expect(page.locator('html')).toHaveAttribute('lang','ru');
  await page.getByLabel('Ключ администратора').fill('ci-only-web-admin-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
  await page.getByRole('button',{name:'Войти'}).click();
  await expect(page.getByRole('heading',{name:'Главная',exact:true,level:1})).toBeVisible();
  await expect(page.getByText('Активность обучения',{exact:true})).toBeVisible();
  await page.screenshot({path:testInfo.outputPath('dashboard.png'),fullPage:true});
  for(const [nav,title] of [['Группы','Группы слов'],['Черновики','Черновики'],['Ученики','Ученики'],['Повторения','Повторения'],['Медиатека','Медиатека'],['Озвучка','Озвучка'],['Изображения','Изображения'],['Бюджеты','Бюджеты'],['Настройки','Настройки']]){
    if(viewport.width<760)await page.getByRole('button',{name:'Открыть меню'}).click();
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
  if(viewport.width<760)await page.getByRole('button',{name:'Открыть меню'}).click();
  await page.locator('nav').getByRole('button',{name:'Группы',exact:true}).click();
  await page.getByRole('button',{name:'Создать группу'}).click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByLabel('Исходный текст урока (TGTeacher TXT)').fill('#FAMILY "Browser '+viewport.width+'"\n#WORD ticket\n#TRANSLATION билет\n');
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
  await page.getByRole('button',{name:'Сохранить редакцию'}).click();
  await expect(page.getByText('Редакция 2',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Проверить редакцию'}).click();
  await expect(page.getByRole('heading',{name:'Сохранённая редакция прошла проверку'})).toBeVisible();
  await expect(page.getByLabel('Перевод',{exact:true})).toHaveValue('проездной билет');
  await page.screenshot({path:testInfo.outputPath('editor.png'),fullPage:true});
  page.once('dialog',d=>d.accept());
  await page.getByRole('button',{name:'Опубликовать',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Группы слов',exact:true,level:1})).toBeVisible();
  await page.getByLabel('Фильтр статуса').selectOption('Published');
  await page.getByPlaceholder('Поиск групп слов…').fill('Browser '+viewport.width);
  await expect(page.getByRole('cell').filter({hasText:'Browser '+viewport.width}).first()).toBeVisible();
  await page.getByRole('row').filter({has:page.getByRole('cell').filter({hasText:'Browser '+viewport.width})}).getByRole('button',{name:'Открыть',exact:true}).click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByRole('button',{name:'История версий'}).click();
  await page.getByRole('button',{name:'Сравнить',exact:true}).first().click();
  await expect(page.getByRole('heading',{name:'Версия 1 → 1'})).toBeVisible();
  await page.getByText('Текущий материал урока',{exact:true}).click();
  await expect(page.getByLabel('Перевод',{exact:true})).toHaveValue('проездной билет');
  await page.getByRole('button',{name:'Закрыть',exact:true}).click();
  await page.getByRole('button',{name:'Выйти'}).click();
  await expect(page.getByRole('button',{name:'Войти'})).toBeVisible();
  expect(browserErrors).toEqual([]);
 });
}
