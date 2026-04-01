# parsers.py
# Объединение трёх парсеров отзывов: CleanDreamJobParser, JobTrueParser, PravdaSotrudnikovParser

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from datetime import datetime
from urllib.parse import urlencode, urljoin, parse_qs, urlparse, urlunparse


# ==================== Парсер для DreamJob ====================
class CleanDreamJobParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        })
        self.base_url = "https://dreamjob.ru"
        self.month_map = {
            'января':1, 'февраля':2, 'марта':3, 'преля':4, 'мая':5, 'июня':6,
            'июля':7, 'августа':8, 'сентября':9, 'октября':10, 'ноября':11, 'декабря':12,
            'январь':1, 'февраль':2, 'март':3, 'апрель':4, 'май':5, 'июнь':6,
            'июль':7, 'август':8, 'сентябрь':9, 'октябрь':10, 'ноябрь':11, 'декабрь':12
        }

    def _normalize_company_name(self, name):
        if not name:
            return ''
        name = re.sub(r'\s*(ООО|ЗАО|ОАО|АО|ИП|ПАО)\s*', ' ', name, flags=re.IGNORECASE).strip()
        name = re.sub(r'\s+', ' ', name)
        return name.lower()

    def search_company(self, company_name):
        search_url = f"{self.base_url}/site/search"
        params = {'query': company_name}
        candidates = []
        try:
            response = self.session.get(search_url, params=params)
            time.sleep(random.uniform(1, 2))
            soup = BeautifulSoup(response.text, 'html.parser')
            company_links = soup.find_all('a', href=re.compile(r'/employers/\d+'))
            for link in company_links:
                if company_name.lower() in link.get_text().lower():
                    href = link.get('href', '')
                    reviews_count = self._extract_reviews_count(link)
                    candidates.append({
                        'name': link.get_text().strip(),
                        'url': f"{self.base_url}{href}" if href.startswith('/') else href,
                        'id': href.split('/')[-1],
                        'reviews_count': reviews_count
                    })
            return candidates
        except Exception as e:
            print(f"❌ Ошибка при поиске компании: {e}")
            return []

    def _extract_reviews_count(self, element):
        parent = element
        for _ in range(10):
            if parent.name and parent.find(string=re.compile(r'отзыв', re.I)):
                text = parent.get_text()
                match = re.search(r'(\d[\d\s]*)\s+отзыв', text, re.I)
                if match:
                    count_str = re.sub(r'\s', '', match.group(1))
                    try:
                        return int(count_str)
                    except:
                        return 0
            parent = parent.parent
            if parent is None:
                break
        return 0

    def find_best_company(self, short_name, trademark):
        norm_short = self._normalize_company_name(short_name)
        norm_trademark = self._normalize_company_name(trademark)
        all_candidates = []

        if norm_short and norm_short == norm_trademark:
            print("🔍 Названия совпадают после нормализации, выполняем один поиск.")
            candidates = self.search_company(short_name)
            if candidates:
                all_candidates.extend(candidates)
        else:
            if norm_short:
                print(f"🔍 Поиск по сокращённому названию: {short_name}")
                candidates1 = self.search_company(short_name)
                if candidates1:
                    all_candidates.extend(candidates1)
            if norm_trademark:
                print(f"🔍 Поиск по товарному знаку: {trademark}")
                candidates2 = self.search_company(trademark)
                if candidates2:
                    existing_urls = {c['url'] for c in all_candidates}
                    for c in candidates2:
                        if c['url'] not in existing_urls:
                            all_candidates.append(c)

        if not all_candidates:
            return None

        best = max(all_candidates, key=lambda x: x.get('reviews_count', 0))
        print(f"🏆 Выбрана компания: {best['name']} (отзывов: {best['reviews_count']})")
        return best

    def get_company_with_fresh_reviews(self, company_url):
        filter_params = {
            'nrs[sort]': '-created_at',
            'nrs[cities]': '[]',
            'nrs[vacancies]': '[]',
            'nrs[departments]': '[]',
            'nrs[ratings]': '[]',
            'nrs[first_selected]': '',
            'nrs[topics][0]': '[]',
            'nrs[topics][1]': '[]',
            'nrs[summary_vacancy]': ''
        }
        return f"{company_url}?{urlencode(filter_params)}"

    def parse_company_reviews(self, company_url, fresh=False):
        try:
            if fresh:
                company_url = self.get_company_with_fresh_reviews(company_url)
            print(f"🔄 Загружаем страницу: {company_url}")
            response = self.session.get(company_url)
            response.raise_for_status()
            time.sleep(random.uniform(1, 2))
            soup = BeautifulSoup(response.text, 'html.parser')

            company_info = self._parse_company_info(soup, company_url)
            reviews = self._parse_clean_reviews(soup)
            company_info['last_review_date'] = self._get_last_review_date(reviews)

            return company_info, reviews
        except Exception as e:
            print(f"❌ Ошибка при парсинге отзывов: {e}")
            return {}, []

    def _parse_company_info(self, soup, company_url):
        info = {'name': 'Неизвестно', 'rating': 'Не указан', 'reviews_count': '0', 'url': company_url}
        try:
            title = soup.find('title')
            if title:
                info['name'] = title.get_text().strip()
            rating_elem = soup.find('div', class_=re.compile(r'rating', re.I))
            if rating_elem:
                info['rating'] = rating_elem.get_text().strip()
            count_elem = soup.find('span', class_=re.compile(r'count', re.I))
            if count_elem:
                info['reviews_count'] = count_elem.get_text().strip()
            else:
                text = soup.get_text()
                match = re.search(r'(\d+)\s+отзыв', text)
                if match:
                    info['reviews_count'] = match.group(1)
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге информации о компании: {e}")
        return info

    # ========== Методы для парсинга отзывов ==========
    def _parse_clean_reviews(self, soup):
        reviews = []
        reviews.extend(self._find_reviews_by_structure(soup))
        if not reviews:
            reviews.extend(self._find_reviews_by_patterns(soup))
        if not reviews:
            reviews.extend(self._find_reviews_by_content(soup))
        print(f"🔍 Найдено отзывов: {len(reviews)}")
        return reviews

    def _find_reviews_by_structure(self, soup):
        reviews = []
        key_phrases = ['Что нравится?', 'Что можно улучшить?', 'Пожаловаться', 'Работаю', 'Работал']
        for phrase in key_phrases:
            elements = soup.find_all(string=re.compile(re.escape(phrase), re.IGNORECASE))
            for element in elements:
                block = self._find_review_container(element)
                if block:
                    review = self._parse_clean_review_block(block)
                    if review and review not in reviews:
                        reviews.append(review)
        return reviews

    def _find_review_container(self, element):
        for parent in element.parents:
            if parent.name in ['div', 'article', 'section']:
                text = parent.get_text()
                if any(phrase in text for phrase in ['Что нравится?', 'Что можно улучшить?']):
                    return parent
        return None

    def _parse_clean_review_block(self, block):
        try:
            text = block.get_text(separator=' \n ', strip=True)
            if len(text) < 50 or 'Сортировать' in text or 'Оценка компании' in text:
                return None
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            review = {
                'position': 'Не указана',
                'experience': 'Не указан',
                'date': 'Не указана',
                'pros': '',
                'cons': '',
                'text': ''
            }
            current_section = None
            pros_lines = []
            cons_lines = []

            for i, line in enumerate(lines):
                if i == 0 and line and not any(word in line for word in ['Пожаловаться', 'Работа', 'Что нравится']):
                    review['position'] = line
                    continue
                if any(word in line for word in ['Работаю', 'Работал']):
                    review['experience'] = line
                    continue
                if re.search(r'20\d{2}', line):
                    review['date'] = line
                    continue
                if 'Что нравится?' in line:
                    current_section = 'pros'
                    continue
                elif 'Что можно улучшить?' in line:
                    current_section = 'cons'
                    continue
                elif 'Ответ представителя компании' in line or 'Полезный отзыв' in line or 'Преимущества и льготы' in line:
                    break

                if current_section == 'pros' and line and not self._is_garbage_line(line):
                    pros_lines.append(line)
                elif current_section == 'cons' and line and not self._is_garbage_line(line):
                    cons_lines.append(line)

            clean_pros = self._clean_pros_text(' '.join(pros_lines))
            clean_cons = self._clean_cons_text(' '.join(cons_lines))

            full_text = []
            if clean_pros:
                full_text.append(f"Что нравится? {clean_pros}")
            if clean_cons:
                full_text.append(f"Что можно улучшить? {clean_cons}")

            review['pros'] = clean_pros
            review['cons'] = clean_cons
            review['text'] = self._final_clean_text(' '.join(full_text))

            return review if review['position'] != 'Не указана' and review['text'] else None
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге блока отзыва: {e}")
            return None

    def _is_garbage_line(self, line):
        garbage_patterns = [
            r'Преимущества и льготы',
            r'Своевременная оплата труда',
            r'Удобное расположение работы',
            r'Удаленная работа',
            r'Наличие кухни',
            r'Медицинское страхование',
            r'Оплата больничного',
            r'Гибкий рабочий график',
            r'Компенсация питания',
            r'Оплата транспортных расходов',
            r'Корпоративный транспорт',
            r'Профессиональное обучение',
            r'Место для парковки',
            r'Оплата мобильной связи',
            r'Система наставничества',
            r'Корпоративные мероприятия',
            r'Полезный отзыв\s*\d*',
            r'Ссылка на отзыв',
            r'Ответить от лица компании'
        ]
        return any(re.search(pattern, line, re.IGNORECASE) for pattern in garbage_patterns)

    def _clean_pros_text(self, text):
        if not text:
            return ""
        clean_text = text
        garbage_phrases = [
            'Преимущества и льготы', 'Своевременная оплата труда', 'Удобное расположение работы',
            'Удаленная работа', 'Наличие кухни', 'Медицинское страхование', 'Оплата больничного',
            'Гибкий рабочий график', 'Компенсация питания', 'Оплата транспортных расходов',
            'Корпоративный транспорт', 'Профессиональное обучение', 'Место для парковки',
            'Оплата мобильной связи', 'Система наставничества', 'Корпоративные мероприятия'
        ]
        for phrase in garbage_phrases:
            clean_text = re.sub(re.escape(phrase), '', clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text

    def _clean_cons_text(self, text):
        if not text:
            return ""
        clean_text = re.split(r'Преимущества и льготы', text, flags=re.IGNORECASE)[0]
        garbage_phrases = [
            'Своевременная оплата труда', 'Удобное расположение работы', 'Удаленная работа',
            'Наличие кухни', 'Медицинское страхование', 'Оплата больничного', 'Гибкий рабочий график',
            'Компенсация питания', 'Оплата транспортных расходов', 'Корпоративный транспорт',
            'Профессиональное обучение', 'Место для парковки', 'Оплата мобильной связи',
            'Система наставничества', 'Корпоративные мероприятия', 'Полезный отзыв',
            'Ссылка на отзыв', 'Ответить от лица компании'
        ]
        for phrase in garbage_phrases:
            clean_text = re.sub(re.escape(phrase), '', clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text

    def _final_clean_text(self, text):
        if not text:
            return ""
        clean_text = text
        garbage_patterns = [
            r'\s*Полезный отзыв\s*\d*\s*',
            r'\s*Ссылка на отзыв\s*',
            r'\s*Ответить от лица компании\s*',
            r'\s*Ответ представителя компании.*',
            r'\s*Преимущества и льготы.*'
        ]
        for pattern in garbage_patterns:
            clean_text = re.sub(pattern, '', clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text

    def _find_reviews_by_patterns(self, soup):
        reviews = []
        all_text = soup.get_text()
        pattern = r'([^\n]+?)\s*Пожаловаться\s*(Работа[^\n]+)\s*([^\n]*20\d{2}[^\n]*)\s*Что нравится\?\s*([^?]+?)\s*Что можно улучшить\?\s*([^?]+?)'
        matches = re.findall(pattern, all_text, re.DOTALL)
        for match in matches:
            position, experience, date, pros, cons = match
            clean_pros = self._clean_pros_text(pros.strip())
            clean_cons = self._clean_cons_text(cons.strip())
            review = {
                'position': position.strip(),
                'experience': experience.strip(),
                'date': date.strip(),
                'pros': clean_pros,
                'cons': clean_cons,
                'text': f"Что нравится? {clean_pros} Что можно улучшить? {clean_cons}"
            }
            reviews.append(review)
        return reviews

    def _find_reviews_by_content(self, soup):
        reviews = []
        divs = soup.find_all('div', class_=True)
        for div in divs:
            text = div.get_text(separator=' \n ', strip=True)
            if (('Что нравится?' in text and 'Что можно улучшить?' in text) and
                len(text) > 100 and
                not any(word in text for word in ['Сортировать', 'Оценка компании', 'отзывов рекомендуют'])):
                review = self._parse_clean_review_block(div)
                if review:
                    reviews.append(review)
        return reviews

    def _parse_review_date(self, date_str):
        date_str = date_str.strip().replace('\u202f', ' ')
        parts = date_str.split()
        if len(parts) == 2:
            month_str, year_str = parts
            try:
                year = int(year_str)
                month = self.month_map.get(month_str.lower())
                if month:
                    return datetime(year, month, 1)
            except:
                pass
        return None

    def _get_last_review_date(self, reviews):
        dates = []
        for r in reviews:
            date_str = r.get('date', '')
            if date_str:
                dt = self._parse_review_date(date_str)
                if dt:
                    dates.append(dt)
        return max(dates) if dates else None

    def save_to_excel(self, company_info, reviews, filename=None):
        pass


# ==================== Парсер для JobTrue ====================
class JobTrueParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        })
        self.base_url = "https://jobtrue.ru"
        self.month_map = {
            'января':1, 'февраля':2, 'марта':3, 'апреля':4, 'мая':5, 'июня':6,
            'июля':7, 'августа':8, 'сентября':9, 'октября':10, 'ноября':11, 'декабря':12,
            'январь':1, 'февраль':2, 'март':3, 'апрель':4, 'май':5, 'июнь':6,
            'июль':7, 'август':8, 'сентябрь':9, 'октябрь':10, 'ноябрь':11, 'декабрь':12
        }

    def _get_letters(self):
        ru = [chr(i) for i in range(ord('А'), ord('Я')+1)]
        en = [chr(i) for i in range(ord('A'), ord('Z')+1)]
        return ru + en + ['#']

    def search_company(self, company_name):
        name_lower = company_name.strip().lower()
        first_letter = name_lower[0].upper() if name_lower else ''
        letters_to_try = [first_letter] if first_letter in self._get_letters() else self._get_letters()
        candidates = []

        for letter in letters_to_try:
            print(f"  Проверяем букву {letter}...")
            url = urljoin(self.base_url, "/company/")
            params = {'letter': letter}
            try:
                resp = self.session.get(url, params=params)
                resp.raise_for_status()
                time.sleep(random.uniform(1, 2))
                soup = BeautifulSoup(resp.text, 'html.parser')
                container = soup.find('div', id='company-list-container')
                if not container:
                    container = soup.find('div', class_='companies-abc-list')
                if not container:
                    continue
                items = container.find_all('a', href=True)
                for link in items:
                    comp_name = link.get_text(strip=True)
                    # Возвращаем точное совпадение после нормализации (убираем лишние символы)
                    norm_comp = re.sub(r'\s+', ' ', comp_name).strip().lower()
                    if norm_comp == name_lower:
                        full_url = urljoin(self.base_url, link['href'])
                        candidates.append({
                            'name': comp_name,
                            'url': full_url
                        })
                if candidates:
                    return candidates
            except Exception as e:
                print(f"⚠️ Ошибка при запросе буквы {letter}: {e}")
            time.sleep(0.5)
        return []

    def find_best_company(self, short_name, trademark):
        def normalize(name):
            if not name:
                return ''
            name = re.sub(r'\s*(ООО|ЗАО|ОАО|АО|ИП|ПАО)\s*', ' ', name, flags=re.IGNORECASE).strip()
            name = re.sub(r'\s+', ' ', name)
            return name.lower()

        norm_short = normalize(short_name)
        norm_trademark = normalize(trademark)
        all_candidates = []

        if norm_short and norm_short == norm_trademark:
            print("🔍 Названия совпадают после нормализации, выполняем один поиск.")
            candidates = self.search_company(short_name)
            if candidates:
                all_candidates.extend(candidates)
        else:
            if norm_short:
                print(f"🔍 Поиск по сокращённому названию: {short_name}")
                candidates1 = self.search_company(short_name)
                if candidates1:
                    all_candidates.extend(candidates1)
            if norm_trademark:
                print(f"🔍 Поиск по товарному знаку: {trademark}")
                candidates2 = self.search_company(trademark)
                if candidates2:
                    existing_urls = {c['url'] for c in all_candidates}
                    for c in candidates2:
                        if c['url'] not in existing_urls:
                            all_candidates.append(c)

        if not all_candidates:
            return None
        best = all_candidates[0]
        print(f"🏆 Выбрана компания: {best['name']}")
        return best

    def get_filtered_reviews_url(self, company_url, fresh=True):
        if not fresh:
            return company_url
        parsed = list(urlparse(company_url))
        query = dict(parse_qs(parsed[4]))
        query['sort'] = 'date-desc'
        parsed[4] = urlencode(query, doseq=True)
        return urlunparse(parsed)

    def parse_company_reviews(self, company_url, fresh=False):
        target_url = self.get_filtered_reviews_url(company_url, fresh)
        print(f"🔄 Загружаем страницу: {target_url}")

        all_reviews = []
        company_info = {}
        page_num = 1

        while True:
            try:
                resp = self.session.get(target_url)
                resp.raise_for_status()
                time.sleep(random.uniform(1, 2))
                soup = BeautifulSoup(resp.text, 'html.parser')

                if page_num == 1:
                    company_info = self._parse_company_info(soup)

                reviews = self._parse_reviews(soup)
                all_reviews.extend(reviews)

                next_link = soup.find('a', class_='next')
                if next_link and next_link.get('href'):
                    target_url = urljoin(self.base_url, next_link['href'])
                    page_num += 1
                    print(f"  Переход на страницу {page_num}...")
                    time.sleep(1)
                else:
                    break
            except Exception as e:
                print(f"❌ Ошибка при парсинге страницы {target_url}: {e}")
                break

        company_info['last_review_date'] = self._get_last_review_date(all_reviews)
        return company_info, all_reviews

    def _parse_company_info(self, soup):
        info = {
            'name': 'Неизвестно',
            'rating': 'Не указан',
            'reviews_count': '0',
            'url': '',
        }
        try:
            title_tag = soup.find('title')
            if title_tag:
                info['name'] = title_tag.get_text(strip=True)
            name_block = soup.find('div', class_='company__name')
            if name_block:
                info['name'] = name_block.get_text(strip=True)
            rating_elem = soup.find('div', class_='company__indicator-number')
            if rating_elem:
                info['rating'] = rating_elem.get_text(strip=True)
            count_elem = soup.find('span', class_='company__indicator-reviews change-item')
            if count_elem:
                info['reviews_count'] = count_elem.get_text(strip=True)
            else:
                text = soup.get_text()
                match = re.search(r'(\d+)\s+отзыв', text)
                if match:
                    info['reviews_count'] = match.group(1)
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге информации о компании: {e}")
        return info

    def _parse_reviews(self, soup):
        reviews = []
        for block in soup.find_all('div', class_='review'):
            review = self._parse_single_review(block)
            if review and review.get('text'):
                reviews.append(review)
        return reviews

    def _parse_single_review(self, block):
        try:
            author_elem = block.find('span', class_='review__header')
            author = author_elem.get_text(strip=True) if author_elem else 'Неизвестно'

            date_elem = block.find('div', class_='review__date')
            date = date_elem.get_text(strip=True) if date_elem else 'Не указана'

            position_elem = block.find('div', class_='review__title-plus')
            position = position_elem.get_text(strip=True) if position_elem else 'Не указана'

            pros_elem = block.find('div', class_='review__text-positive')
            pros_text = ''
            if pros_elem:
                pros_text = pros_elem.get_text(separator=' ', strip=True)
                pros_text = re.sub(r'Плюсы в работе:', '', pros_text, flags=re.IGNORECASE).strip()

            cons_elem = block.find('div', class_='review__text-negative')
            cons_text = ''
            if cons_elem:
                cons_text = cons_elem.get_text(separator=' ', strip=True)
                cons_text = re.sub(r'Отрицательные стороны:', '', cons_text, flags=re.IGNORECASE).strip()

            full_text = ''
            if pros_text:
                full_text += f"Плюсы: {pros_text}. "
            if cons_text:
                full_text += f"Минусы: {cons_text}."

            pros_clean = re.sub(r'\s+', ' ', pros_text).strip()
            cons_clean = re.sub(r'\s+', ' ', cons_text).strip()
            full_clean = re.sub(r'\s+', ' ', full_text).strip()

            return {
                'author': author,
                'position': position,
                'date': date,
                'pros': pros_clean,
                'cons': cons_clean,
                'text': full_clean
            }
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге отзыва: {e}")
            return None

    def _parse_review_date(self, date_str):
        date_str = date_str.strip()
        parts = date_str.split()
        if len(parts) == 2:
            month_str, year_str = parts
            try:
                year = int(year_str)
                month = self.month_map.get(month_str.lower())
                if month:
                    return datetime(year, month, 1)
            except:
                pass
        return None

    def _get_last_review_date(self, reviews):
        dates = []
        for r in reviews:
            date_str = r.get('date', '')
            if date_str:
                dt = self._parse_review_date(date_str)
                if dt:
                    dates.append(dt)
        return max(dates) if dates else None

    def calculate_review_scores(self, company_info):
        pass

    def save_to_excel(self, company_info, reviews, filename=None):
        pass


# ==================== Парсер для PravdaSotrudnikov ====================
class PravdaSotrudnikovParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        })
        self.base_url = "https://pravda-sotrudnikov.ru"

    def _normalize_company_name(self, name):
        if not name:
            return ''
        name = re.sub(r'\s*(ООО|ЗАО|ОАО|АО|ИП|ПАО)\s*', ' ', name, flags=re.IGNORECASE).strip()
        name = re.sub(r'\s+', ' ', name)
        return name.lower()

    def search_company(self, company_name):
        search_url = urljoin(self.base_url, "/search")
        params = {'q': company_name}
        candidates = []
        try:
            print(f"🔍 Запрашиваем: {search_url} с параметрами {params}")
            response = self.session.get(search_url, params=params)
            response.raise_for_status()
            time.sleep(random.uniform(1, 2))
            soup = BeautifulSoup(response.text, 'html.parser')
            company_links = soup.find_all('a', href=re.compile(r'/company/'))
            for link in company_links:
                href = link.get('href', '')
                if '/company/insert' in href:
                    continue
                name = link.get_text(strip=True)
                if name and href:
                    full_url = urljoin(self.base_url, href)
                    if not any(c['url'] == full_url for c in candidates):
                        candidates.append({'name': name, 'url': full_url})
            print(f"✅ Найдено компаний: {len(candidates)}")
            return candidates
        except Exception as e:
            print(f"❌ Ошибка при поиске компании: {e}")
            return []

    def _get_company_views(self, company_url):
        try:
            resp = self.session.get(company_url)
            resp.raise_for_status()
            time.sleep(random.uniform(1, 2))
            soup = BeautifulSoup(resp.text, 'html.parser')
            views_elem = soup.find('span', class_='company-info-views-count')
            if views_elem:
                views_text = views_elem.get_text(strip=True)
                views = re.sub(r'\D', '', views_text)
                return int(views) if views else 0
            return 0
        except Exception as e:
            print(f"⚠️ Не удалось получить просмотры для {company_url}: {e}")
            return 0

    def find_best_company(self, short_name, trademark, max_candidates=8):
        norm_short = self._normalize_company_name(short_name)
        norm_trademark = self._normalize_company_name(trademark)
        all_candidates = []

        if norm_short and norm_short == norm_trademark:
            print("🔍 Названия совпадают после нормализации, выполняем один поиск.")
            candidates = self.search_company(short_name)
            if candidates:
                all_candidates.extend(candidates)
        else:
            if norm_short:
                print(f"🔍 Поиск по сокращённому названию: {short_name}")
                candidates1 = self.search_company(short_name)
                if candidates1:
                    all_candidates.extend(candidates1)
            if norm_trademark:
                print(f"🔍 Поиск по товарному знаку: {trademark}")
                candidates2 = self.search_company(trademark)
                if candidates2:
                    existing_urls = {c['url'] for c in all_candidates}
                    for c in candidates2:
                        if c['url'] not in existing_urls:
                            all_candidates.append(c)

        if not all_candidates:
            return None

        candidates_to_analyze = all_candidates[:max_candidates]
        print(f"🔎 Анализируем {len(candidates_to_analyze)} компаний для выбора лучшей...")
        best = None
        max_views = -1
        for comp in candidates_to_analyze:
            views = self._get_company_views(comp['url'])
            print(f"   {comp['name']} – {views} просмотров")
            if views > max_views:
                max_views = views
                best = comp
            time.sleep(random.uniform(1.5, 3))
        if best:
            print(f"🏆 Выбрана компания: {best['name']} ({max_views} просмотров)")
            best['views'] = max_views
        return best

    def parse_company_page(self, company_url):
        url = company_url + "?sort=date"
        try:
            print(f"🔄 Загружаем страницу: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            time.sleep(random.uniform(1, 2))
            soup = BeautifulSoup(response.text, 'html.parser')

            company_info = self._parse_company_info(soup, company_url)
            reviews = self._parse_reviews_from_page(soup)

            pagination = soup.find('ul', class_='pagination')
            total_pages = 1
            if pagination:
                page_links = pagination.find_all('a', href=True)
                page_numbers = []
                for link in page_links:
                    href = link.get('href', '')
                    match = re.search(r'[?&]page=(\d+)', href)
                    if match:
                        page_numbers.append(int(match.group(1)))
                if page_numbers:
                    total_pages = max(page_numbers)

            return company_info, reviews, total_pages
        except Exception as e:
            print(f"❌ Ошибка при загрузке страницы компании: {e}")
            return {}, [], 1

    def _parse_company_info(self, soup, company_url):
        info = {
            'name': 'Неизвестно',
            'rating': 'Не указан',
            'reviews_count': '0',
            'url': company_url,
            'last_review_date': None
        }
        try:
            h1 = soup.find('h1')
            if h1:
                info['name'] = h1.get_text(strip=True)
            rating_span = soup.find('span', class_='rating-autostars')
            if rating_span:
                rating_val = rating_span.get('data-rating', '')
                if rating_val:
                    info['rating'] = rating_val
            reviews_title = soup.find('div', class_='company-reviews-title')
            if reviews_title:
                text = reviews_title.get_text()
                match = re.search(r'\((\d+)\)', text)
                if match:
                    info['reviews_count'] = match.group(1)
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге информации о компании: {e}")
        return info

    def _parse_reviews_from_page(self, soup):
        # Ищем блоки отзывов разными способами
        review_blocks = soup.find_all('div', class_='company-reviews-list-item')
        if not review_blocks:
            review_blocks = soup.find_all('div', class_='review-item')
        if not review_blocks:
            review_blocks = soup.find_all('div', class_='review')
        print(f"🔍 Найдено блоков отзывов на странице: {len(review_blocks)}")
        reviews = []
        for idx, block in enumerate(review_blocks):
            try:
                review = self._parse_single_review(block)
                if not review:
                    print(f"⚠️ Отзыв {idx+1}: review == None, class={block.get('class')}")
                if review:
                    reviews.append(review)
                else:
                    print(f"⚠️ Отзыв {idx+1}: не удалось распарсить (вернул None)")
            except Exception as e:
                print(f"⚠️ Отзыв {idx+1}: ошибка: {e}")
        print(f"✅ Успешно распарсено отзывов: {len(reviews)}")
        return reviews

    def _parse_single_review(self, block):
        # Автор и статус
        author = "Аноним"
        status = ""
        name_div = block.find('div', class_='company-reviews-list-item-name')
        if name_div:
            text = name_div.get_text(strip=True)
            match = re.search(r'\(([^)]+)\)', text)
            if match:
                status = match.group(1).strip()
            else:
                author = text
        # Город
        city = ""
        city_div = block.find('div', class_='company-reviews-list-item-city')
        if city_div:
            city_text = city_div.get_text(strip=True)
            city = re.sub(r'^Город:\s*', '', city_text)
        # Дата
        date = ""
        date_div = block.find('div', class_='company-reviews-list-item-date')
        if date_div:
            date = date_div.get_text(strip=True)
        else:
            time_tag = block.find('time')
            if time_tag and time_tag.get('datetime'):
                date = time_tag.get('datetime')
        # Плюсы и минусы
        pros = ""
        cons = ""
        # Пробуем найти блоки с плюсами/минусами
        text_blocks = block.find_all('div', class_='company-reviews-list-item-text')
        for tb in text_blocks:
            title_div = tb.find('div', class_='company-reviews-list-item-text-title')
            if title_div:
                title = title_div.get_text(strip=True)
                if 'Плюсы' in title:
                    pros = self._extract_review_text(tb)
                elif 'Отрицательные' in title or 'Минусы' in title:
                    cons = self._extract_review_text(tb)
        # Если не нашли, пробуем альтернативные классы
        if not pros and not cons:
            pros_div = block.find('div', class_='review__text-positive')
            if pros_div:
                pros = self._extract_review_text(pros_div)
            cons_div = block.find('div', class_='review__text-negative')
            if cons_div:
                cons = self._extract_review_text(cons_div)
        # Если всё ещё пусто, берём весь текст блока
        if not pros and not cons:
            # Ищем любой div с текстом (например, company-reviews-list-item-text-message)
            msg_div = block.find('div', class_='company-reviews-list-item-text-message')
            if not msg_div:
                msg_div = block.find('div', class_='review__text')
            if msg_div:
                full_text = msg_div.get_text(separator=' ', strip=True)
            else:
                full_text = block.get_text(separator=' ', strip=True)
            if full_text:
                pros = full_text
        # Формируем полный текст
        full_text = ""
        if pros:
            full_text += f"Плюсы: {pros}. "
        if cons:
            full_text += f"Минусы: {cons}."
        if not full_text:
            return None
        return {
            'author': author,
            'status': status,
            'city': city,
            'date': date,
            'pros': pros,
            'cons': cons,
            'text': full_text
        }

    def _extract_review_text(self, block):
        if not block:
            return ''
        # Ищем сообщение в сворачиваемом блоке
        msg_div = block.find('div', class_='company-reviews-list-item-text-message')
        if not msg_div:
            msg_div = block.find('div', class_='review__text')
        if not msg_div:
            msg_div = block
        # Ищем полный текст внутри collapsible-body (даже если он скрыт)
        collapsible = block.find('div', class_='collapsible-body')
        if collapsible:
            text = collapsible.get_text(separator='\n', strip=True)
        else:
            text = msg_div.get_text(separator='\n', strip=True)
        text = re.sub(r'\n\s*\n', '\n', text).strip()
        # Убираем лишние заголовки
        text = re.sub(r'Плюсы:\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Минусы:\s*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Отрицательные стороны:\s*', '', text, flags=re.IGNORECASE)
        return text

    def _parse_review_date(self, date_str):
        date_str = date_str.strip()
        try:
            return datetime.strptime(date_str, "%H:%M %d.%m.%Y")
        except ValueError:
            pass
        try:
            return datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            return None

    def _get_last_review_date(self, reviews):
        dates = []
        for r in reviews:
            if r.get('date'):
                dt = self._parse_review_date(r['date'])
                if dt:
                    dates.append(dt)
        return max(dates) if dates else None

    def get_fresh_reviews(self, company_url, max_pages=2, delay=2):
        company_info, reviews_first, total_pages = self.parse_company_page(company_url)
        all_reviews = reviews_first.copy()
        company_info['last_review_date'] = self._get_last_review_date(all_reviews)
        pages_to_fetch = min(max_pages, total_pages)
        for page in range(2, pages_to_fetch + 1):
            print(f"📄 Загружаем страницу {page} из {pages_to_fetch}...")
            page_url = f"{company_url}?sort=date&page={page}"
            try:
                time.sleep(random.uniform(delay, delay+1))
                resp = self.session.get(page_url)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')
                page_reviews = self._parse_reviews_from_page(soup)
                all_reviews.extend(page_reviews)
            except Exception as e:
                print(f"⚠️ Ошибка при загрузке страницы {page}: {e}")
        company_info['last_review_date'] = self._get_last_review_date(all_reviews)
        return company_info, all_reviews

    def parse_company_reviews(self, company_url, fresh=False):
        if fresh:
            company_info, reviews = self.get_fresh_reviews(company_url, max_pages=2, delay=2)
        else:
            company_info, reviews, _ = self.parse_company_page(company_url)
        return company_info, reviews

    def calculate_review_scores(self, company_info, reviews):
        pass

    def save_to_excel(self, company_info, reviews, filename=None):
        pass