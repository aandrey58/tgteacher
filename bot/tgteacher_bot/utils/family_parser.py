import re
from typing import List, Dict, Any
import os

VALID_TARGETS = {"FREE", "VIP", "VIP+FREE"}

def parse_family_txt(filepath: str) -> Dict[str, Any]:
    with open(filepath, encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('//')]

    family = {
        'name': None, 'description': None, 'words': [], 
        'stage2_tasks': [], 'stage3_tasks': [], 'stage4_tasks': [], 'stage5_tasks': [], 'stage6_tasks': [], 'stage7_tasks': [], 'stage8_tasks': [],
        'target': 'VIP+FREE'
    }
    
    current_stage = 1
    current_word = {} # for stage 1
    current_task = {} # for stage 2
    s4_current_task = {} # for stage 4
    s3_current_task = {} # for stage 3
    s5_current_task = {} # for stage 5
    s6_current_task = {} # for stage 6
    s7_current_task = {} # for stage 7
    s8_text_lines = []
    s8_answers = []
    s8_explanations = []
    s8_equal = None

    i = 0
    while i < len(lines):
        line = lines[i]

        if line.startswith('#STAGE'):
            # Commit previous stage's data
            if current_word: family['words'].append(current_word)
            if current_task: family['stage2_tasks'].append(current_task)
            if s4_current_task: family['stage4_tasks'].append(s4_current_task)
            if s3_current_task: family['stage3_tasks'].append(s3_current_task)
            if s5_current_task: family['stage5_tasks'].append(s5_current_task)
            if s6_current_task: family['stage6_tasks'].append(s6_current_task)
            if s7_current_task: family['stage7_tasks'].append(s7_current_task)
            if s8_text_lines:
                # Новый формат: text, answers, explanations, equal
                s8_task = {'text': '\\n'.join(s8_text_lines)}
                if s8_answers:
                    s8_task['answers'] = s8_answers
                if s8_explanations:
                    s8_task['explanations'] = s8_explanations
                if s8_equal:
                    s8_task['equal'] = s8_equal
                family['stage8_tasks'].append(s8_task)

            # Reset parsers
            current_word, current_task, s3_current_task, s6_current_task, s4_current_task = {}, {}, {}, {}, {}
            s5_current_task = {}
            s7_current_task = {}
            s8_text_lines, s8_answers, s8_explanations, s8_equal = [], [], [], None

            m = re.match(r'#STAGE(\d+)', line)
            if not m: raise ValueError(f"Ошибка в строке {i+1}: некорректный формат #STAGE")
            current_stage = int(m.group(1))
            i += 1
            continue

        if line.startswith('#TARGET'):
            raw = line.replace('#TARGET', '').strip().upper()
            if raw not in VALID_TARGETS:
                raise ValueError(f'Ошибка в строке {i+1}: некорректный #TARGET (ожидается FREE, VIP или VIP+FREE)')
            family['target'] = raw
        elif line.startswith('#FAMILY'):
            m = re.match(r'#FAMILY\s+"(.+?)"', line)
            if not m: raise ValueError(f'Ошибка в строке {i+1}: некорректный #FAMILY')
            family['name'] = m.group(1)
        elif line.startswith('#DESCRIPTION'):
            family['description'] = line.replace('#DESCRIPTION', '').strip()
        
        # STAGE 1
        elif current_stage == 1:
            if line.startswith('#WORD'):
                if current_word: family['words'].append(current_word)
                current_word = {'word': line.replace('#WORD', '').strip()}
            elif line.startswith('#TRANSLATION'):
                current_word['translation'] = line.replace('#TRANSLATION', '').strip()
            elif line.startswith('#EXAMPLE_TRANSLATION'):
                current_word['example_translation'] = line.replace('#EXAMPLE_TRANSLATION', '').strip() or ""
            elif line.startswith('#EXAMPLE'):
                current_word['example'] = line.replace('#EXAMPLE', '').strip()
            elif line.startswith('#HINT'):
                current_word['hint'] = line.replace('#HINT', '').strip()

        # STAGE 2
        elif current_stage == 2:
            if line.startswith('#TASK'):
                if current_task: family['stage2_tasks'].append(current_task)
                current_task = {}
            elif line.startswith('#SENTENCE'):
                current_task['sentence'] = line.replace('#SENTENCE', '').strip()
            elif line.startswith('#ANSWER'):
                current_task['answer'] = line.replace('#ANSWER', '').strip()
            elif line.startswith('#CHOICES'):
                current_task['choices'] = [c.strip() for c in line.replace('#CHOICES', '').strip().split(';')]
            elif line.startswith('#EXPLANATION'):
                current_task['explanation'] = line.replace('#EXPLANATION', '').strip()
        
        # STAGE 4 (аналог STAGE 2)
        elif current_stage == 4:
            if line.startswith('#TASK'):
                if s4_current_task: family['stage4_tasks'].append(s4_current_task)
                s4_current_task = {}
            elif line.startswith('#SENTENCE'):
                s4_current_task['sentence'] = line.replace('#SENTENCE', '').strip()
            elif line.startswith('#ANSWER'):
                s4_current_task['answer'] = line.replace('#ANSWER', '').strip()
            elif line.startswith('#CHOICES'):
                s4_current_task['choices'] = [c.strip() for c in line.replace('#CHOICES', '').strip().split(';')]
            elif line.startswith('#EXPLANATION'):
                s4_current_task['explanation'] = line.replace('#EXPLANATION', '').strip()

        # STAGE 3
        elif current_stage == 3:
            if line.startswith('#WORD'):
                if s3_current_task: family['stage3_tasks'].append(s3_current_task)
                s3_current_task = {'word': line.replace('#WORD', '').strip()}
            elif line.startswith('#DEFINITION'):
                s3_current_task['definition'] = line.replace('#DEFINITION', '').strip()
            elif line.startswith('#EXPLANATION'):
                s3_current_task['explanation'] = line.replace('#EXPLANATION', '').strip()
            elif line.startswith('#CHOICES'):
                s3_current_task['choices'] = [c.strip() for c in line.replace('#CHOICES', '').strip().split(';') if c.strip()]

        # STAGE 5 (картинка + ввод слова)
        elif current_stage == 5:
            if line.startswith('#TASK'):
                if s5_current_task: family['stage5_tasks'].append(s5_current_task)
                s5_current_task = {}
            elif line.startswith('#IMAGE'):
                s5_current_task['image'] = line.replace('#IMAGE', '').strip()
            elif line.startswith('#ANSWER'):
                s5_current_task['answer'] = line.replace('#ANSWER', '').strip()
            elif line.startswith('#ALTERNATIVES'):
                raw = line.replace('#ALTERNATIVES', '').strip()
                s5_current_task['alternatives'] = [c.strip() for c in raw.split(';') if c.strip()]
            elif line.startswith('#EXPLANATION'):
                s5_current_task['explanation'] = line.replace('#EXPLANATION', '').strip()

        # STAGE 6
        elif current_stage == 6:
            if line.startswith('#WORD'):
                if s6_current_task: family['stage6_tasks'].append(s6_current_task)
                s6_current_task = {'word': line.replace('#WORD', '').strip()}
            elif line.startswith('#VAR'):
                s6_current_task['var_count'] = int(line.replace('#VAR', '').strip())
            elif line.startswith('#SYNONYMS'):
                s6_current_task['synonyms'] = [s.strip() for s in line.replace('#SYNONYMS', '').strip().split(',')]
            elif line.startswith('#WRONG_SYNONYMS'):
                s6_current_task['wrong_synonyms'] = [s.strip() for s in line.replace('#WRONG_SYNONYMS', '').strip().split(',')]
            elif line.startswith('#EXPLANATION'):
                s6_current_task['explanation'] = line.replace('#EXPLANATION', '').strip()
        
        # STAGE 7 (аудио + выбор из кнопок)
        elif current_stage == 7:
            if line.startswith('#TASK'):
                if s7_current_task: family['stage7_tasks'].append(s7_current_task)
                s7_current_task = {}
            elif line.startswith('#AUDIO'):
                s7_current_task['audio'] = line.replace('#AUDIO', '').strip()
            elif line.startswith('#ANSWER'):
                s7_current_task['answer'] = line.replace('#ANSWER', '').strip()
            elif line.startswith('#CHOICES'):
                raw = line.replace('#CHOICES', '').strip()
                s7_current_task['choices'] = [c.strip() for c in raw.split(';') if c.strip()]
            elif line.startswith('#EXPLANATION'):
                s7_current_task['explanation'] = line.replace('#EXPLANATION', '').strip()

        # STAGE 8
        elif current_stage == 8:
            if line.startswith('#TEXT'):
                pass # just a marker
            elif re.match(r'#ANSWER\d+', line):
                # #ANSWERn Слово
                s8_answers.append(line.split(' ', 1)[1].strip())
            elif re.match(r'#EXPLANATION\d+', line):
                # #EXPLANATIONn Пояснение
                s8_explanations.append(line.split(' ', 1)[1].strip())
            elif line.startswith('#EQUAL'):
                # #EQUAL 1=2, 3=7
                s8_equal = line.replace('#EQUAL', '').strip()
            else:
                s8_text_lines.append(line)

        i += 1

    # Commit any remaining data
    if current_word: family['words'].append(current_word)
    if current_task: family['stage2_tasks'].append(current_task)
    if s4_current_task: family['stage4_tasks'].append(s4_current_task)
    if s3_current_task: family['stage3_tasks'].append(s3_current_task)
    if s5_current_task: family['stage5_tasks'].append(s5_current_task)
    if s6_current_task: family['stage6_tasks'].append(s6_current_task)
    if s7_current_task: family['stage7_tasks'].append(s7_current_task)
    if s8_text_lines:
        s8_task = {'text': '\\n'.join(s8_text_lines)}
        if s8_answers:
            s8_task['answers'] = s8_answers
        if s8_explanations:
            s8_task['explanations'] = s8_explanations
        if s8_equal:
            s8_task['equal'] = s8_equal
        family['stage8_tasks'].append(s8_task)
    
    if not family['name']: raise ValueError('Не найден #FAMILY в файле')
    if not family['words']: raise ValueError('Нет ни одного слова в группе словы')

    return family


def load_all_families_from_dir(path: str) -> List[Dict[str, Any]]:
    families = []
    if not os.path.isdir(path):
        return families
    for subdir in os.listdir(path):
        subdir_path = os.path.join(path, subdir)
        if os.path.isdir(subdir_path):
            for fname in os.listdir(subdir_path):
                if fname.endswith('.txt') and fname != 'family_example.txt':
                    fpath = os.path.join(subdir_path, fname)
                    try:
                        fam = parse_family_txt(fpath)
                        families.append(fam)
                    except Exception as e:
                        print(f'[FAMILY LOAD ERROR] {subdir}/{fname}: {e}')
    return families 