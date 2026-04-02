import pandas as pd
import re
import os
import requests
import threading
import random
import time
from datetime import datetime
from llm_classifier import LLM_Classifier
import config as llm_config  

from rules import critical_check, additional_data, calculate_score
from parsers import CleanDreamJobParser, JobTrueParser, PravdaSotrudnikovParser

# =============================================
# КОНСТАНТЫ
# =============================================

MAX_FIN_SCORE = 102
MAX_BUS_SCORE = 85
MAX_LEGAL_SCORE = 100

GOOGLE_SCRIPT_URL = os.environ.get("GOOGLE_SCRIPT_URL")
if not GOOGLE_SCRIPT_URL:
    print("⚠️ Не задан GOOGLE_SCRIPT_URL, логирование отключено")

ENABLE_DREAMJOB = os.environ.get("ENABLE_DREAMJOB", "1").lower() in ("1", "true", "yes")
print(f"DEBUG: ENABLE_DREAMJOB = {ENABLE_DREAMJOB}")

ENABLE_LLM = os.environ.get("ENABLE_LLM", "1").lower() in ("1", "true", "yes")
ENABLE_BANNER = os.environ.get("ENABLE_BANNER", "1").lower() in ("1", "true", "yes")
print(f"DEBUG: ENABLE_LLM = {ENABLE_LLM}")
print(f"DEBUG: ENABLE_BANNER = {ENABLE_BANNER}")

ENABLE_LOGGING = os.environ.get("ENABLE_LOGGING", "1").lower() in ("1", "true", "yes")
print(f"DEBUG: ENABLE_LOGGING = {ENABLE_LOGGING}")

# =============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================
def parse_review_date(date_str):
    """Универсальный парсер даты отзыва для разных форматов."""
    if not date_str:
        return None
    # Формат: "HH:MM DD.MM.YYYY" (PravdaSotrudnikov)
    match = re.match(r'(\d{2}):(\d{2}) (\d{2})\.(\d{2})\.(\d{4})', date_str)
    if match:
        h, m, d, mo, y = match.groups()
        return datetime(int(y), int(mo), int(d), int(h), int(m))
    # Формат: "DD.MM.YYYY" (возможен)
    match = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if match:
        d, mo, y = match.groups()
        return datetime(int(y), int(mo), int(d))
    # Формат: "Месяц ГГГГ" (JobTrue)
    parts = date_str.split()
    if len(parts) == 2:
        month_name, year_str = parts
        year = int(year_str)
        month_map = {
            'января':1, 'февраля':2, 'марта':3, 'апреля':4, 'мая':5, 'июня':6,
            'июля':7, 'августа':8, 'сентября':9, 'октября':10, 'ноября':11, 'декабря':12,
            'январь':1, 'февраль':2, 'март':3, 'апрель':4, 'май':5, 'июнь':6,
            'июль':7, 'август':8, 'сентябрь':9, 'октябрь':10, 'ноябрь':11, 'декабрь':12
        }
        month = month_map.get(month_name.lower())
        if month:
            return datetime(year, month, 1)
    return None

def format_risk_value(param_name, value):
    if value is None:
        return "нет данных"
    if isinstance(value, str):
        return value
    param_lower = param_name.lower()
    if isinstance(value, (int, float)):
        # Добавляем % для динамических показателей
        if "динамика" in param_lower:
            return f"{value:.1f}%"
        if any(kw in param_lower for kw in ('рентабельность', 'доля', 'рост', 'темп', 'маржинальность')):
            return f"{value:.1f}%"
        if any(kw in param_lower for kw in ('оборачиваемость', 'цикл', 'период')):
            return f"{int(round(value))} дн."
        if any(kw in param_lower for kw in ('коэффициент', 'ликвидность','ликвидности','независимости', 'соотношение')):
            return f"{value:.2f}"
        if value > 1000:
            return f"{int(value):,}".replace(",", " ")
        if isinstance(value, float):
            if value > 100:
                return str(int(round(value)))
            else:
                return f"{value:.1f}".rstrip('0').rstrip('.') if '.' in f"{value:.1f}" else str(value)
        return str(value)
    return str(value)


def generate_risk_comment(param_name, value, group=None):
    param_lower = param_name.lower()
    value_str = str(value) if value is not None else "отсутствует"

    # Критические
    if "ликвидац" in param_lower:
        return "Компания в процессе ликвидации — лучше поискать другое место."
    if "банкротств" in param_lower:
        return "Банкротство — лучше поискать другое место."
    if "недостоверные сведения по адресу" in param_lower:
        return "Юридический адрес недостоверен — возможны проблемы с оформлением."
    if "недостоверные сведения по директору" in param_lower or "недостоверные сведения по учредителю" in param_lower:
        return "Данные о руководителе/учредителе вызывают сомнения — компания может быть ненадёжной."
    if "массовый директор" in param_lower:
        return "У директора много компаний — возможно, он просто номинальное лицо."
    if "массовый юридический адрес" in param_lower:
        return "Адрес массовой регистрации — типичный признак фирм-однодневок."
    if "дисквалификация" in param_lower:
        return "Руководитель был дисквалифицирован — серьёзный знак."
    if "недобросовестных поставщиков" in param_lower:
        return "Компания в реестре недобросовестных поставщиков — с контрагентами нечистоплотна."
    if "исполнительные производства" in param_lower and "существенные" in param_lower:
        return "Крупные долги — зарплата может быть под угрозой."
    if "отчётности за последний год" in param_lower:
        return "Нет отчётности — возможно, компания не ведёт реальной деятельности."
    if "однодневной" in param_lower:
        return "Признаки фирмы-однодневки — лучше держаться подальше."

    # Финансовые
    if "динамика выручки" in param_lower:
        return "Выручка падает — о премии и повышении можно забыть."
    if "валовая рентабельность" in param_lower or "рентабельность продаж" in param_lower:
        return "Низкая маржинальность — компания едва сводит концы с концами."
    if "динамика чистой прибыли" in param_lower:
        return "Чистая прибыль снижается — возможны сокращения."
    if "динамика собственного капитала" in param_lower:
        return "Капитал тает — компания теряет устойчивость."
    if "коэффициент финансовой независимости" in param_lower:
        return "Высокая зависимость от займов — при кризисе могут задержать зарплату."
    if "чистый оборотный капитал" in param_lower:
        return "Не хватает оборотных средств — возможны перебои с выплатами."
    if "рентабельность собственного капитала" in param_lower or "roe" in param_lower:
        return "Низкая отдача на капитал — бизнес неэффективен."
    if "коэффициент краткосрочной ликвидности" in param_lower:
        return "Проблемы с краткосрочной ликвидностью — могут не выплатить зарплату вовремя."
    if "оборачиваемость оборотного капитала" in param_lower:
        return "Деньги застревают в обороте — компания медленно платит."
    if "обеспеченность сос" in param_lower:
        return "Нехватка собственных средств — риск банкротства."
    if "динамика операционного цикла" in param_lower:
        return "Цикл производства затягивается — возможно, задерживают оплату."
    if "оборачиваемость дебиторской задолженности" in param_lower or "оборачиваемость дебиторки" in param_lower:
        return "Клиенты долго платят — компании может не хватать денег на зарплату."
    if "оборачиваемость запасов" in param_lower:
        return "Товары залёживаются — компания теряет прибыль."
    if "оборачиваемость кредиторской задолженности" in param_lower or "оборачиваемость кредиторки" in param_lower:
        return "Компания задерживает оплату поставщикам — с сотрудниками может быть так же."
    if "общий долг / ebit" in param_lower or "общий долг/ebit" in param_lower:
        val = value if isinstance(value, (int, float)) else 0
        return f"Долги компании превышают её годовую прибыль в {val:.1f} раз — критическая долговая нагрузка, высокий риск банкротства."
    if "коэффициент общей ликвидности" in param_lower:
        return "Низкая ликвидность — возможны задержки зарплаты."
    if "чп / дельта нераспред. прибыли" in param_lower:
        return "Прибыль расходится не по назначению — возможны махинации."
    if "доля финансовых вложений" in param_lower:
        return "Слишком много денег вложено в ценные бумаги, а не в развитие — возможны спекуляции."
    if "рентабельность по чистой прибыли" in param_lower:
        val = value if isinstance(value, (int, float)) else 0
        return f"Чистая прибыль составляет всего {val:.1f}% от выручки — бизнес работает на грани рентабельности."
    if "ebit / проценты" in param_lower or "ebit/проценты" in param_lower:
        val = value if isinstance(value, (int, float)) else 0
        return f"Прибыль компании едва покрывает проценты по кредитам ({val:.2f}) — высокий риск дефолта и проблем с выплатами."
    if "динамика ebit" in param_lower:
        val = value if isinstance(value, (int, float)) else 0
        return f"Операционная прибыль снижается на {val:.1f}% — компания теряет доходность."
    
    # Деловая активность
    if "возраст компании" in param_lower:
        return "Компания слишком молода — нет гарантий стабильности."
    if "участие в госзакупках" in param_lower:
        return "Нет опыта госзакупок — возможно, не хватает опыта."
    if "история проверок" in param_lower and "нарушения" in value_str:
        return "Были нарушения при проверках — к компании есть вопросы."
    if "история изменений компании" in param_lower and "изменения" in value_str:
        return "Частая смена руководителей/учредителей — нестабильность."

    # Правовые
    if "арбитражные дела" in param_lower:
        return "Много судов — компания постоянно с кем-то судится."
    if "исполнительные производства" in param_lower and not "существенные" in param_lower:
        return "Есть долги по исполнительным производствам."
    if "налоговая задолженность" in param_lower:
        return "Долги перед налоговой — могут арестовать счета."
    if "лицензии" in param_lower:
        return "Отсутствует обязательная лицензия — работа может быть незаконной."

    # Отзывы
    if "оценка компании" in param_lower:
        return f"Рейтинг на сайтах отзывов: {value_str} — сотрудники не очень довольны."
    if "количество отзывов" in param_lower:
        return f"Мало отзывов ({value_str}) — возможно, компания скрывает проблемы."
    if "дата последнего отзыва" in param_lower:
        return f"Последний отзыв был {value_str} — давно не появлялось новых мнений."
    if "количество отзывов" in param_lower:
        if value == 0:
            return "На сайте найдена карточка компании, но отзывы отсутствуют. Это может означать, что компания неактивна или скрывает проблемы."
        else:
            return f"Мало отзывов ({value})."    

    return f"Выявлен риск: {param_name} ({value_str})."


# =============================================
# ФУНКЦИИ ЛОГИРОВАНИЯ
# =============================================

def send_check_log(log_data):
        if not ENABLE_LOGGING:
            return  
        """Отправляет лог проверки в Google Sheets."""
        print(f"DEBUG: отправка лога с полями: {list(log_data.keys())}")
        if 'comment_1' in log_data:
            print(f"DEBUG: comment_1: {log_data['comment_1'][:100]}")
        if not GOOGLE_SCRIPT_URL:
            return
        try:
            response = requests.post(GOOGLE_SCRIPT_URL, json=log_data, timeout=10)
            response.raise_for_status()
            print("✅ Лог проверки отправлен в Google Sheets")
        except Exception as e:
            print(f"⚠️ Ошибка отправки лога проверки: {e}")

def send_banner_log(session_id, action, report_number):
    if not ENABLE_LOGGING:
        return
    """Отправляет действие баннера в Google Sheets."""
    if not GOOGLE_SCRIPT_URL:
        return
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'session_id': session_id,
        'action': action,
        'report_number': report_number,
        'type': 'banner_action'
    }
    try:
        response = requests.post(GOOGLE_SCRIPT_URL, json=log_data, timeout=10)
        response.raise_for_status()
        print("✅ Баннер-действие отправлено в Google Sheets")
    except Exception as e:
        print(f"⚠️ Ошибка отправки баннер-действия: {e}")


# =============================================
# АГРЕГАЦИЯ ОТЗЫВОВ
# =============================================
def analyze_reviews_with_llm(reviews_list, auth_key=None):
    if not reviews_list:
        return None
    # Преобразуем в формат, ожидаемый классификатором
    llm_input = []
    for idx, rev in enumerate(reviews_list):
        text = rev.get('text') or ''
        if text:
            llm_input.append({
                'review': text,
                'business_id': 'current_company'
            })
    if not llm_input:
        return None

    classifier = LLM_Classifier()
    if not auth_key:
        auth_key = os.getenv('GCH_AUTH_KEY')
    if not auth_key:
        print("⚠️ Отсутствует ключ авторизации GigaChat. Анализ отзывов невозможен.")
        return None

    try:
        result = classifier.basic_pipeline(
            list_with_reviews=llm_input,
            aggregate_by_business_ID=True,
            business_id_key='business_id',
            auth_key=auth_key
        )
        # result будет словарь вида {'current_company': {...}}
        if result and isinstance(result, dict) and 'current_company' in result:
            return result['current_company']
        else:
            return None
    except Exception as e:
        print(f"❌ Ошибка при вызове LLM: {e}")
        import traceback
        traceback.print_exc()
        return None
    

def aggregate_reviews(inn, name_variants):
    parsers = []
    if ENABLE_DREAMJOB:
        parsers.append(CleanDreamJobParser())
    parsers.append(JobTrueParser())
    parsers.append(PravdaSotrudnikovParser())

    short_name = name_variants.get('brand_cleaned_short', '')
    trademark = name_variants.get('brand_manual') or name_variants.get('brand_domain') or short_name

    print(f"\n🔍 DEBUG: Поиск отзывов для ИНН {inn}")
    print(f"   short_name = '{short_name}'")
    print(f"   trademark  = '{trademark}'")

    reviews_agg = {'ratings': [], 'total_count': 0, 'last_dates': [], 'all_reviews': []}

    for idx, parser in enumerate(parsers):
        parser_name = parser.__class__.__name__
        print(f"\n--- Запуск {parser_name} ---")
        try:
            best = parser.find_best_company(short_name, trademark)
            if not best:
                print(f"⚠️ {parser_name}: компания не найдена")
                continue
            print(f"✅ {parser_name}: найдена компания '{best['name']}' -> {best['url']}")

            info, reviews = parser.parse_company_reviews(best['url'], fresh=True)
            if not info:
                print(f"⚠️ {parser_name}: не удалось получить информацию о компании")
                continue

            print(f"   Рейтинг: {info.get('rating')}")
            print(f"   Кол-во отзывов: {info.get('reviews_count')}")
            print(f"   Последний отзыв: {info.get('last_review_date')}")
            print(f"   Найдено отзывов на странице: {len(reviews)}")

            rating_str = info.get('rating', '')
            # Универсальное извлечение числа: ищем любую последовательность цифр, возможно с точкой или запятой
            rating_match = re.search(r'(\d+(?:[.,]\d+)?)', rating_str)
            if rating_match:
                rating = float(rating_match.group(1).replace(',', '.'))
                reviews_agg['ratings'].append(rating)
            else:
                print(f"⚠️ Не удалось извлечь рейтинг из строки: {rating_str}")

            count_str = info.get('reviews_count', '0')
            count_digits = re.sub(r'\D', '', count_str)
            if count_digits:
                reviews_agg['total_count'] += int(count_digits)

            last_date = info.get('last_review_date')
            if last_date:
                reviews_agg['last_dates'].append(last_date)

            reviews_agg['all_reviews'].extend(reviews)

        except Exception as e:
            print(f"❌ Ошибка в {parser_name}: {e}")

        if idx < len(parsers) - 1:
            delay = random.uniform(2, 5)
            print(f"⏳ Пауза {delay:.1f} сек...")
            time.sleep(delay)

    if not reviews_agg['ratings']:
        print("❌ Отзывы не найдены ни одним парсером")
        return None

    print(f"\n📊 Агрегированные данные:")
    print(f"   Рейтинги: {reviews_agg['ratings']}")
    print(f"   Суммарное кол-во отзывов: {reviews_agg['total_count']}")
    print(f"   Последние даты: {reviews_agg['last_dates']}")

    final_rating = min(reviews_agg['ratings'])
    final_count = reviews_agg['total_count']
    final_last_date = max(reviews_agg['last_dates']) if reviews_agg['last_dates'] else None

    if reviews_agg and reviews_agg.get('reviews'):
        print(f"DEBUG: будет отправлено {len(reviews_agg['reviews'])} комментариев в лог")
        for idx, rev in enumerate(reviews_agg['reviews'][:3], 1):
            print(f"DEBUG: comment_{idx}: {rev.get('text', '')[:100]}")
    else:
        print("DEBUG: reviews_agg['reviews'] пуст или отсутствует")

    # распарсим даты для всех отзывов (добавим временное поле)
    for rev in reviews_agg['all_reviews']:
        dt = parse_review_date(rev.get('date', ''))
        rev['_date_dt'] = dt if dt else datetime.min

    # Сортируем по распарсенной дате (новые сверху)
    sorted_reviews = sorted(reviews_agg['all_reviews'], key=lambda r: r['_date_dt'], reverse=True)[:10]


    # Ограничиваем количество отзывов для LLM (например, 3–5)
    MAX_LLM_REVIEWS = 5
    llm_reviews = sorted_reviews[:MAX_LLM_REVIEWS]

    # LLM анализ
    reviews_llm_summary = None
    if ENABLE_LLM and sorted_reviews:
        reviews_for_llm = []
        for rev in sorted_reviews:
            if not isinstance(rev, dict):
                continue
            text_parts = []
            if rev.get('pros'):
                text_parts.append(f"Плюсы: {rev['pros']}")
            if rev.get('cons'):
                text_parts.append(f"Минусы: {rev['cons']}")
            if not text_parts:
                text_parts.append(rev.get('text', ''))
            full_text = " ".join(text_parts).strip()
            if full_text:
                reviews_for_llm.append({'text': full_text})
        if reviews_for_llm:
            print(f"DEBUG: отправляем в LLM {len(reviews_for_llm)} отзывов")
            try:
                reviews_llm_summary = analyze_reviews_with_llm(reviews_for_llm)
                print(f"DEBUG: LLM вернул {reviews_llm_summary}")
            except Exception as e:
                print(f"ERROR in LLM: {e}")
                import traceback
                traceback.print_exc()
                reviews_llm_summary = None
    else:
        if not ENABLE_LLM:
            print("LLM анализ отключён (ENABLE_LLM=False)")

    return {
        'rating': final_rating,
        'count': final_count,
        'last_date': final_last_date.strftime('%d.%m.%Y') if final_last_date else None,
        'reviews': sorted_reviews,
        'llm_summary': reviews_llm_summary
    }


def calculate_reviews_score(agg):
    score = 0
    details = []

    if agg['rating'] > 4.5:
        score += 35
        details.append(('Оценка компании', agg['rating'], 35))
    elif 3.9 <= agg['rating'] <= 4.5:
        score += 20
        details.append(('Оценка компании', agg['rating'], 20))
    else:
        score += 0
        details.append(('Оценка компании', agg['rating'], 0))

    if agg['count'] > 100:
        score +=35
        details.append(('Количество отзывов', agg['count'], 35))
    elif agg['count'] >= 10:
        score += 20
        details.append(('Количество отзывов', agg['count'], 20))
    else:
        score += 0
        details.append(('Количество отзывов', agg['count'], 0))

    if agg['last_date']:
        try:
            last = datetime.strptime(agg['last_date'], '%d.%m.%Y')
            days_old = (datetime.now() - last).days
            if days_old < 180:
                score += 30
                details.append(('Дата последнего отзыва', agg['last_date'], 30))
            else:
                score += 0
                details.append(('Дата последнего отзыва', agg['last_date'], 0))
        except:
            score += 0
            details.append(('Дата последнего отзыва', 'некорректная', 0))
    else:
        score += 0
        details.append(('Дата последнего отзыва', 'нет данных', 0))

    return score, details


# =============================================
# ОСНОВНАЯ ФУНКЦИЯ ПРОВЕРКИ
# =============================================

def check_company(inn, session_id, report_number):
    start_time = datetime.now().isoformat()

    status, critical_list, data, name_variants, is_largest, is_ip = critical_check(inn)

    log_data = {
        'start_time': start_time,
        'inn': inn,
        'is_largest': is_largest,
        'short_name': name_variants.get('brand_cleaned_short', ''),
        'brand_name': name_variants.get('brand_manual') or name_variants.get('brand_domain', ''),
        'session_id': session_id,
        'report_number': report_number
    }

    if status == 'critical':
        risk_params = []
        for crit in critical_list:
            risk_params.append({
                'group': 'Критические риски',
                'name': crit,
                'value': 'Присутствует',
                'comment': generate_risk_comment(crit, True)
            })

        log_data.update({
            'end_time': datetime.now().isoformat(),
            'status': 'critical',
            'critical_params': ', '.join(critical_list)
        })
        threading.Thread(target=send_check_log, args=(log_data,)).start()

        return {
            'status': 'critical',
            'critical_params': critical_list,
            'risk_params': risk_params,
            'message': 'Обнаружены критические риски',
            'company_name': name_variants,
            'is_largest': is_largest,
            'report_number': report_number,
            'enable_banner': ENABLE_BANNER
        }

    data = additional_data(data)
    groups_scores_ofdata, high_risk_params, details_df, extra = calculate_score(data)

    original_fin_score = groups_scores_ofdata['Финансовое состояние']
    if is_largest:
        groups_scores_ofdata['Финансовое состояние'] = 100
        large_company_message = "Компания относится к крупнейшим, влияние финансовых показателей на риски работы в ней незначительны"
    else:
        large_company_message = None

    fin_norm = (groups_scores_ofdata['Финансовое состояние'] / MAX_FIN_SCORE) * 100
    bus_norm = (groups_scores_ofdata['Деловая активность и опыт'] / MAX_BUS_SCORE) * 100
    legal_norm = (groups_scores_ofdata['Правовые риски'] / MAX_LEGAL_SCORE) * 100


    reviews_agg = aggregate_reviews(inn, name_variants)
    reviews_llm_summary = None

    if reviews_agg is None:
        # Компания не найдена на сайтах отзывов
        reviews_score = 50
        reviews_details = []
        reviews_message = "Отзывы о компании не найдены"
    else:
        if reviews_agg.get('count', 0) == 0:
            # Карточка есть, но отзывов нет
            reviews_score = 50
            reviews_details = []
            reviews_message = "На сайте найдена карточка компании, но отзывы отсутствуют"
        else:
            # Есть отзывы, считаем обычный скор
            reviews_score, reviews_details = calculate_reviews_score(reviews_agg)
            reviews_message = None
            reviews_llm_summary = reviews_agg.get('llm_summary')

    total_score = 0.5 * fin_norm + 0.1 * bus_norm + 0.1 * legal_norm + 0.3 * reviews_score

    if total_score >= 75:
        risk_level = 'low'
        risk_text = 'Риски отсутствуют'
    elif total_score >= 50:
        risk_level = 'medium'
        risk_text = 'Средние риски'
    else:
        risk_level = 'high'
        risk_text = 'Высокие риски'

    risk_params = []
    for param in high_risk_params:
        formatted_value = format_risk_value(param['параметр'], param['значение'])
        comment = generate_risk_comment(param['параметр'], param['значение'], param['группа'])
        if is_largest and param['группа'] == 'Финансовое состояние':
            comment += " (для крупной компании этот показатель менее критичен)"
        risk_params.append({
            'group': param['группа'],
            'name': param['параметр'],
            'value': formatted_value,
            'comment': comment
        })
    for detail in reviews_details:
        if detail[2] in (0,):
            formatted_value = format_risk_value(detail[0], detail[1])
            comment = generate_risk_comment(detail[0], detail[1], 'Отзывы')
            risk_params.append({
                'group': 'Отзывы',
                'name': detail[0],
                'value': formatted_value,
                'comment': comment
            })

    log_data.update({
        'end_time': datetime.now().isoformat(),
        'status': 'completed',
        'reviews_rating': reviews_agg['rating'] if reviews_agg else None,
        'reviews_count': reviews_agg['count'] if reviews_agg else None,
        'reviews_last_date': reviews_agg['last_date'] if reviews_agg else None,
        'financial_score': round(original_fin_score if not is_largest else 1, 1),
        'business_score': round(bus_norm, 1),
        'legal_score': round(legal_norm, 1),
        'reviews_score': round(reviews_score, 1),
        'total_score': round(total_score, 1),
    })

    if details_df is not None:
        for _, row in details_df.iterrows():
            param_name = re.sub(r'[^\w\s]', '', row['Параметр']).strip()
            log_data[param_name] = row['Значение']

    if reviews_agg and reviews_agg.get('reviews'):
        for idx, rev in enumerate(reviews_agg['reviews'][:10], 1):
            comment_text = rev.get('text', '')[:5000]
            log_data[f'comment_{idx}'] = comment_text

    threading.Thread(target=send_check_log, args=(log_data,)).start()

    return {
        'status': 'ok',
        'risk_level': risk_level,
        'risk_text': risk_text,
        'total_score': round(total_score, 1),
        'risk_params': risk_params,
        'company_name': name_variants,
        'reviews_found': reviews_agg is not None,
        'reviews_message': reviews_message,
        'reviews_agg': reviews_agg,   # здесь теперь есть поле llm_summary
        'groups_scores': {
            'financial': round(fin_norm, 1),
            'business': round(bus_norm, 1),
            'legal': round(legal_norm, 1),
            'reviews': round(reviews_score, 1)
        },
        'large_company_message': large_company_message,
        'extra': extra,
        'report_number': report_number,
        'enable_banner': ENABLE_BANNER  
    }

def send_visit_log(session_id, referrer, user_agent):
    if not ENABLE_LOGGING:
        return
    """Отправляет событие визита в Google Sheets."""
    if not GOOGLE_SCRIPT_URL:
        return
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'session_id': session_id,
        'referrer': referrer,
        'user_agent': user_agent,
        'type': 'visit'
    }
    try:
        response = requests.post(GOOGLE_SCRIPT_URL, json=log_data, timeout=10)
        response.raise_for_status()
        print("✅ Визит отправлен в Google Sheets")
    except Exception as e:
        print(f"⚠️ Ошибка отправки визита: {e}")