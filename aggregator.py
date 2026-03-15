import pandas as pd
import re
import os
import requests
import threading
import random
import time
from datetime import datetime

# Импортируем функции из rules.py и парсеры
from rules import critical_check, additional_data, calculate_score
from parsers import CleanDreamJobParser, JobTrueParser, PravdaSotrudnikovParser

# =============================================
# КОНСТАНТЫ
# =============================================

# Максимальные баллы для нормализации групп (рассчитаны по коду rules.py)
MAX_FIN_SCORE = 102   # финансовое состояние
MAX_BUS_SCORE = 85    # деловая активность
MAX_LEGAL_SCORE = 100 # правовые риски

# URL для логирования в Google Sheets (берётся из переменной окружения)
GOOGLE_SCRIPT_URL = os.environ.get("GOOGLE_SCRIPT_URL")
if not GOOGLE_SCRIPT_URL:
    print("⚠️ Не задан GOOGLE_SCRIPT_URL, логирование отключено")

# Флаг включения DreamJob парсера (по умолчанию включён)
ENABLE_DREAMJOB = os.environ.get("ENABLE_DREAMJOB", "1").lower() in ("1", "true", "yes")
print(f"DEBUG: ENABLE_DREAMJOB = {ENABLE_DREAMJOB} (raw: {os.environ.get('ENABLE_DREAMJOB')})")


# =============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================

def format_risk_value(param_name, value):
    """
    Форматирует числовое значение параметра для читаемости.
    Добавляет %, дни, округления в зависимости от контекста.
    """
    if value is None:
        return "нет данных"
    if isinstance(value, str):
        return value
    param_lower = param_name.lower()
    if isinstance(value, (int, float)):
        # Проценты
        if any(kw in param_lower for kw in ('рентабельность', 'доля', 'рост', 'темп', 'маржинальность')):
            return f"{value:.1f}%"
        # Дни
        if any(kw in param_lower for kw in ('оборачиваемость', 'дн', 'цикл', 'период')):
            return f"{int(round(value))} дн."
        # Коэффициенты (обычно < 10)
        if any(kw in param_lower for kw in ('коэффициент', 'ликвидность', 'независимости', 'соотношение')):
            return f"{value:.2f}"
        # Большие суммы
        if value > 1000:
            return f"{int(value):,}".replace(",", " ")
        # Обычные числа
        if isinstance(value, float):
            if value > 100:
                return str(int(round(value)))
            else:
                return f"{value:.1f}".rstrip('0').rstrip('.') if '.' in f"{value:.1f}" else str(value)
        return str(value)
    return str(value)


def generate_risk_comment(param_name, value, group=None):
    """
    Возвращает короткое пояснение для параметра риска, ориентированное на соискателя.
    """
    param_lower = param_name.lower()
    value_str = str(value) if value is not None else "отсутствует"

    # Критические проверки
    if "ликвидац" in param_lower:
        return "Компания в процессе ликвидации — оформление и зарплата под вопросом."
    if "банкротств" in param_lower:
        return "Банкротство — лучше поискать другое место, здесь могут задерживать зарплату."
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
    if "оборачиваемость дебиторской задолженности" in param_lower:
        return "Клиенты долго платят — компании не хватает денег на зарплату."
    if "оборачиваемость запасов" in param_lower:
        return "Товары залёживаются — компания теряет прибыль."
    if "оборачиваемость кредиторской задолженности" in param_lower or "оборачиваемость кредиторки" in param_lower:
        return "Компания задерживает оплату поставщикам — с сотрудниками может быть так же."
    if "общий долг / ebit" in param_lower:
        return "Долги сильно превышают прибыль — высокий риск дефолта."
    if "коэффициент общей ликвидности" in param_lower:
        return "Низкая ликвидность — возможны задержки зарплаты."
    if "чп / дельта нераспред. прибыли" in param_lower:
        return "Прибыль расходится не по назначению — возможны махинации."
    if "ebit / проценты" in param_lower:
        return "Компания с трудом обслуживает проценты по кредитам — финансовая нагрузка высока."
    if "доля финансовых вложений" in param_lower:
        return "Слишком много денег вложено в ценные бумаги, а не в развитие — возможны спекуляции."

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

    # Универсальный
    return f"Выявлен риск: {param_name} ({value_str})."


def send_log_to_google_sheets(log_data):
    """Отправляет словарь log_data в Google Sheets через POST (в фоновом потоке)."""
    if not GOOGLE_SCRIPT_URL:
        return
    try:
        response = requests.post(GOOGLE_SCRIPT_URL, json=log_data, timeout=10)
        response.raise_for_status()
        print("✅ Лог отправлен в Google Sheets")
    except Exception as e:
        print(f"⚠️ Ошибка отправки лога: {e}")


# =============================================
# АГРЕГАЦИЯ ОТЗЫВОВ
# =============================================

def aggregate_reviews(inn, name_variants):
    """
    Собирает отзывы с сайтов и агрегирует метрики.
    Возвращает dict с агрегированными данными и список отзывов.
    """
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

    reviews_agg = {
        'ratings': [],
        'total_count': 0,
        'last_dates': [],
        'all_reviews': []
    }

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

            # Извлекаем метрики
            rating_str = info.get('rating', '')
            rating_match = re.search(r'(\d+[.,]\d+)', rating_str)
            if rating_match:
                rating = float(rating_match.group(1).replace(',', '.'))
                reviews_agg['ratings'].append(rating)

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

        # Задержка между парсерами
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

    final_rating = min(reviews_agg['ratings'])  # наихудшая оценка
    final_count = reviews_agg['total_count']
    final_last_date = max(reviews_agg['last_dates']) if reviews_agg['last_dates'] else None

    # Сортировка отзывов по дате (свежие сверху) и берём 10
    sorted_reviews = sorted(
        reviews_agg['all_reviews'],
        key=lambda r: r.get('date', ''),
        reverse=True
    )[:10]

    return {
        'rating': final_rating,
        'count': final_count,
        'last_date': final_last_date.strftime('%d.%m.%Y') if final_last_date else None,
        'reviews': sorted_reviews
    }


def calculate_reviews_score(agg):
    """
    Рассчитывает скор по отзывам согласно таблице.
    agg: dict с ключами rating, count, last_date (строка даты)
    """
    score = 0
    details = []

    # Оценка компании
    if agg['rating'] > 4.5:
        score += 0
        details.append(('Оценка компании', agg['rating'], 0))
    elif 3.9 <= agg['rating'] <= 4.5:
        score += 20
        details.append(('Оценка компании', agg['rating'], 20))
    else:
        score += 35
        details.append(('Оценка компании', agg['rating'], 35))

    # Количество отзывов
    if agg['count'] > 100:
        score += 0
        details.append(('Количество отзывов', agg['count'], 0))
    elif agg['count'] >= 10:
        score += 20
        details.append(('Количество отзывов', agg['count'], 20))
    else:
        score += 35
        details.append(('Количество отзывов', agg['count'], 35))

    # Дата последнего отзыва
    if agg['last_date']:
        try:
            last = datetime.strptime(agg['last_date'], '%d.%m.%Y')
            days_old = (datetime.now() - last).days
            if days_old < 180:
                score += 0
                details.append(('Дата последнего отзыва', agg['last_date'], 0))
            else:
                score += 30
                details.append(('Дата последнего отзыва', agg['last_date'], 30))
        except:
            score += 30
            details.append(('Дата последнего отзыва', 'некорректная', 30))
    else:
        score += 30
        details.append(('Дата последнего отзыва', 'нет данных', 30))

    return score, details


# =============================================
# ОСНОВНАЯ ФУНКЦИЯ ПРОВЕРКИ
# =============================================

def check_company(inn):
    """
    Основная функция: получает данные из ofdata, парсит отзывы, считает итоговый скор.
    Возвращает словарь с результатами для фронтенда.
    """
    start_time = datetime.now().isoformat()

    # Шаг 1: критическая проверка (ofdata)
    status, critical_list, data, name_variants, is_largest, is_ip = critical_check(inn)

    # Подготовка базовых данных для лога
    log_data = {
        'start_time': start_time,
        'inn': inn,
        'is_largest': is_largest,
        'short_name': name_variants.get('brand_cleaned_short', ''),
        'brand_name': name_variants.get('brand_manual') or name_variants.get('brand_domain', '')
    }

    # Если критический статус – возвращаем результат сразу
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
        threading.Thread(target=send_log_to_google_sheets, args=(log_data,)).start()

        return {
            'status': 'critical',
            'critical_params': critical_list,
            'risk_params': risk_params,
            'message': 'Обнаружены критические риски',
            'company_name': name_variants,
            'is_largest': is_largest
        }

    # Шаг 2: дополнительные данные и расчёт скора ofdata
    data = additional_data(data)
    groups_scores_ofdata, high_risk_params, details_df, extra = calculate_score(data)

    # --- НОВОЕ: для крупных компаний финансовый скор заменяем на 1 ---
    original_fin_score = groups_scores_ofdata['Финансовое состояние']
    if is_largest:
        groups_scores_ofdata['Финансовое состояние'] = 1  # минимальный риск
        large_company_message = "Компания относится к крупнейшим, влияние финансовых показателей на риски работы в ней незначительны"
    else:
        large_company_message = None

    # Нормализация групп до 0-100
    fin_norm = (groups_scores_ofdata['Финансовое состояние'] / MAX_FIN_SCORE) * 100
    bus_norm = (groups_scores_ofdata['Деловая активность и опыт'] / MAX_BUS_SCORE) * 100
    legal_norm = (groups_scores_ofdata['Правовые риски'] / MAX_LEGAL_SCORE) * 100

    # Шаг 3: сбор отзывов
    reviews_agg = aggregate_reviews(inn, name_variants)

    if reviews_agg is None:
        # Отзывы не найдены: максимальный риск по этому блоку
        reviews_score = 100
        reviews_details = [('Отзывы', 'не найдены', 100)]
        reviews_message = "Отзывы о компании не найдены"
    else:
        reviews_score, reviews_details = calculate_reviews_score(reviews_agg)
        reviews_message = None

    # Шаг 4: итоговый скор с весами
    total_score = 0.3 * fin_norm + 0.2 * bus_norm + 0.2 * legal_norm + 0.3 * reviews_score

    # Определение уровня риска
    if total_score < 25:
        risk_level = 'low'
        risk_text = 'Риски отсутствуют'
    elif total_score < 75:
        risk_level = 'medium'
        risk_text = 'Средние риски'
    else:
        risk_level = 'high'
        risk_text = 'Высокие риски'

    # Формирование списка параметров с высоким риском (с комментариями)
    risk_params = []
    # Из ofdata
    for param in high_risk_params:
        formatted_value = format_risk_value(param['параметр'], param['значение'])
        comment = generate_risk_comment(param['параметр'], param['значение'], param['группа'])
        # Для крупных компаний можно добавить пометку к финансовым параметрам
        if is_largest and param['группа'] == 'Финансовое состояние':
            comment += " (для крупной компании этот показатель менее критичен)"
        risk_params.append({
            'group': param['группа'],
            'name': param['параметр'],
            'value': formatted_value,
            'comment': comment
        })
    # Из отзывов: добавляем те, которые дают максимальный балл (35 или 30)
    for detail in reviews_details:
        if detail[2] in (35, 30):  # высокий балл
            formatted_value = format_risk_value(detail[0], detail[1])
            comment = generate_risk_comment(detail[0], detail[1], 'Отзывы')
            risk_params.append({
                'group': 'Отзывы',
                'name': detail[0],
                'value': formatted_value,
                'comment': comment
            })

    # Подготовка лога (сохраняем все параметры и 10 последних комментариев)
    log_data.update({
        'end_time': datetime.now().isoformat(),
        'status': 'completed',
        'reviews_rating': reviews_agg['rating'] if reviews_agg else None,
        'reviews_count': reviews_agg['count'] if reviews_agg else None,
        'reviews_last_date': reviews_agg['last_date'] if reviews_agg else None,
        'financial_score': round(original_fin_score if not is_largest else 1, 1),  # в логах показываем исходный, если был заменён
        'business_score': round(bus_norm, 1),
        'legal_score': round(legal_norm, 1),
        'reviews_score': round(reviews_score, 1),
        'total_score': round(total_score, 1),
    })

    # Добавляем все параметры из details_df (значения)
    if details_df is not None:
        for _, row in details_df.iterrows():
            # Очищаем ключ от пробелов и спецсимволов для заголовка столбца
            param_name = re.sub(r'[^\w\s]', '', row['Параметр']).strip()
            log_data[param_name] = row['Значение']

    # Добавляем 10 последних комментариев в лог
    if reviews_agg and reviews_agg.get('reviews'):
        for idx, rev in enumerate(reviews_agg['reviews'][:10], 1):
            # обрезаем до 500 символов, чтобы не перегружать ячейку
            comment_text = rev.get('text', '')[:500]
            log_data[f'comment_{idx}'] = comment_text

    # Отправляем лог в фоновом потоке
    threading.Thread(target=send_log_to_google_sheets, args=(log_data,)).start()

    # Возвращаем результат для фронтенда
    return {
        'status': 'ok',
        'risk_level': risk_level,
        'risk_text': risk_text,
        'total_score': round(total_score, 1),
        'risk_params': risk_params,
        'company_name': name_variants,
        'reviews_found': reviews_agg is not None,
        'reviews_message': reviews_message,
        'reviews_agg': reviews_agg,
        'groups_scores': {
            'financial': round(fin_norm, 1),
            'business': round(bus_norm, 1),
            'legal': round(legal_norm, 1),
            'reviews': round(reviews_score, 1)
        },
        'large_company_message': large_company_message,
        'extra': extra
    }