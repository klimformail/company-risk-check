import requests
import json
import pandas as pd
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from urllib.parse import urlparse





# ======================== НАСТРОЙКИ ========================
import os
API_KEY = os.environ.get("OFDATA_API_KEY")
if not API_KEY:
    raise ValueError("OFDATA_API_KEY не задан в переменных окружения")
BASE_URL = "https://api.ofdata.ru/v2"
STATUS_CSV = "справочник статусов.csv"

# Ручной справочник брендов (исправленный и дополненный)
MANUAL_BRANDS = {
    "7704217370": "Ozon",                # Интернет Решения
    "7707083893": "Сбер",                 # Сбербанк России
    "7736207543": "Яндекс",                # Яндекс
    "7721546864": "Wildberries",           # Вайлдберриз
    "7710140679": "Т-банк",                # Тбанк (бывш. Тинькофф)
    "7702070139": "ВТБ",                   # ВТБ
    "7712040126": "Аэрофлот",              # Аэрофлот
    "7740000076": "МТС",                   # МТС
    "7812014560": "Мегафон",               # Мегафон
    "7708004767": "Лукойл",                # Лукойл
    "7736050003": "Газпром",               # Газпром
    "7708503727": "РЖД",                   # РЖД
    "7707049388": "Ростелеком",            # Ростелеком
    "7728168971": "Альфа-Банк",            # Альфа-Банк
    "7724490000": "Почта России",          # Почта России
    "7705034202": "PepsiCo",               # Пепсико Холдингс
    "7704282033": "Яндекс.Практикум",      # АНО ДПО «Образовательные Технологии Яндекса»
}

# Список организационно-правовых форм для очистки
LEGAL_PREFIXES = [
    r'ООО', r'ЗАО', r'ОАО', r'ПАО', r'АО', r'ИП', r'ЧУП',
    r'ФГУП', r'ГУП', r'МУП', r'НКО', r'АНО', r'Фонд',
    r'Общество с ограниченной ответственностью',
    r'Закрытое акционерное общество',
    r'Открытое акционерное общество',
    r'Публичное акционерное общество',
    r'Акционерное общество',
    r'Индивидуальный предприниматель',
    r'Федеральное государственное унитарное предприятие',
    r'Государственное унитарное предприятие',
    r'Муниципальное унитарное предприятие',
    r'Некоммерческая организация',
    r'Автономная некоммерческая организация',
]

# Общие слова для удаления (необязательно, но улучшает результат)
COMMON_WORDS_TO_REMOVE = [
    r'Компания', r'Группа', r'Холдинг', r'Корпорация',
    r'Банк', r'Завод', r'Фабрика', r'Комбинат',
]

# ======================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========================
def load_status_dict():
    _status_df = pd.read_csv(STATUS_CSV)
    _status_dict = {}
    for _, row in _status_df.iterrows():
        key = (str(row['code']), row['type'])
        _status_dict[key] = row['status']
    return _status_dict

_status_dict = load_status_dict()

def get_company_status(code: str, entity_type: str = 'LEGAL') -> Optional[str]:
    key = (str(code), entity_type)
    return _status_dict.get(key, None)

def fetch_data(endpoint: str, params: dict) -> Optional[dict]:
    url = f"{BASE_URL}/{endpoint}"
    params["key"] = API_KEY
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("meta", {}).get("status") == "ok":
            return data
        else:
            print(f"[{endpoint}] Ошибка: {data.get('meta', {}).get('message')}")
            return None
    except Exception as e:
        print(f"[{endpoint}] Исключение: {e}")
        return None

def safe_float(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def extract_domain_from_url(url: str) -> Optional[str]:
    """Извлекает корневой домен из URL (без www, без протокола)."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = re.sub(r'^www\.', '', domain)
        # Берём основную часть до первого символа после точки (например, ozon.ru -> ozon)
        main_part = domain.split('.')[0]
        return main_part.capitalize() if main_part else None
    except:
        return None

def clean_short_name(name: str) -> str:
    """Очищает сокращённое наименование от ОПФ и лишних символов."""
    if not name:
        return ""
    name = name.replace('"', '').replace("'", "").replace("«", "").replace("»", "")
    for prefix in LEGAL_PREFIXES:
        pattern = rf'^{re.escape(prefix)}\s+'
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    for word in COMMON_WORDS_TO_REMOVE:
        pattern = rf'\s+{re.escape(word)}\s+'
        name = re.sub(pattern, ' ', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def get_name_variants(company_data: dict, inn: str, is_ip: bool = False) -> dict:
    """
    Возвращает словарь с тремя вариантами названия:
    - brand_manual (из ручного справочника, если есть)
    - brand_domain (из домена сайта, если доступен)
    - brand_cleaned_short (очищенное сокращённое наименование)
    """
    variants = {
        "brand_manual": None,
        "brand_domain": None,
        "brand_cleaned_short": None
    }
    
    if is_ip:
        fio = company_data.get("ФИО") or ""
        variants["brand_cleaned_short"] = fio.replace('"', '').replace("'", "").strip()
        return variants
    
    # 1. Ручной справочник
    if inn in MANUAL_BRANDS:
        variants["brand_manual"] = MANUAL_BRANDS[inn]
    
    # 2. Домен сайта
    website = company_data.get("Контакты", {}).get("ВебСайт")
    if website:
        domain_brand = extract_domain_from_url(website)
        if domain_brand:
            variants["brand_domain"] = domain_brand
    
    # 3. Очищенное сокращённое наименование
    short_name = company_data.get("НаимСокр") or company_data.get("НаимПолн") or ""
    if short_name:
        variants["brand_cleaned_short"] = clean_short_name(short_name)
    
    return variants

# ======================== БЛОК 1: КРИТИЧЕСКИЕ ПРОВЕРКИ ========================
def critical_check(inn: str) -> Tuple[str, List[str], Dict[str, Any], dict, bool, bool]:
    print("Блок 1: получение данных для критических проверок...")
    
    is_ip = (len(inn) == 12)
    
    if is_ip:
        print("Обнаружен ИП, запрос /entrepreneur...")
        ip_resp = fetch_data("entrepreneur", {"inn": inn})
        if not ip_resp:
            raise Exception("Не удалось получить данные ИП – проверьте ИНН или доступ к API.")
        ip_data = ip_resp.get("data", {})
        company = {
            "ИНН": ip_data.get("ИНН"),
            "ОГРН": ip_data.get("ОГРНИП"),
            "НаимСокр": ip_data.get("ФИО"),
            "НаимПолн": ip_data.get("ФИО"),
            "ФИО": ip_data.get("ФИО"),
            "ДатаРег": ip_data.get("ДатаРег"),
            "Статус": ip_data.get("Статус", {}),
            "Ликвид": ip_data.get("Прекращ", {}),
            "Руковод": [],
            "Учред": {"ФЛ": [], "РосОрг": []},
            "МассРуковод": False,
            "МассУчред": False,
            "ДисквЛица": False,
            "НедобПост": ip_data.get("НедобПост", False),
            "Налоги": ip_data.get("Налоги", {}),
            "УстКап": {"Сумма": 0},
            "ЮрАдрес": {"АдресРФ": ip_data.get("НасПункт", ""), "Недост": False, "МассАдрес": []},
            "ОКВЭД": ip_data.get("ОКВЭД", {}),
        }
        is_largest = False
        finances = {}
    else:
        company_resp = fetch_data("company", {"inn": inn})
        if not company_resp:
            raise Exception("Не удалось получить данные компании – проверьте ИНН или доступ к API.")
        company = company_resp.get("data", {})
        
        fin_resp = fetch_data("finances", {"inn": inn, "extended": "true"})
        finances = fin_resp.get("data", {}) if fin_resp else {}
        
        charter_capital = safe_float(company.get("УстКап", {}).get("Сумма")) or 0
        revenue_largest = 0
        if finances:
            years = sorted([int(y) for y in finances.keys() if y.isdigit() and int(y) >= 2020], reverse=True)
            if years:
                latest = years[0]
                rev = None
                yr_data = finances.get(str(latest), {})
                if isinstance(yr_data.get("2110"), dict):
                    rev = safe_float(yr_data["2110"].get("СумОтч"))
                else:
                    rev = safe_float(yr_data.get("2110"))
                if rev:
                    revenue_largest = rev
        is_largest = (revenue_largest > 20e9) or (charter_capital > 5e9)
    
    # Исполнительные производства
    enf_resp = fetch_data("enforcements", {"inn": inn, "limit": 100})
    enforcements = enf_resp.get("data", {}).get("Записи", []) if enf_resp else []
    
    # --- Вычисление критических флагов ---
    critical_flags = {}
    
    # Статус ликвидации/прекращения
    status_code = company.get("Статус", {}).get("Код")
    entity_type_for_status = 'INDIVIDUAL' if is_ip else 'LEGAL'
    company_status = get_company_status(status_code, entity_type_for_status) if status_code else None
    critical_flags["Статус ликвидации"] = (company_status in ["LIQUIDATING", "LIQUIDATED"])
    critical_flags["Статус банкротства"] = (company_status == "BANKRUPT")
    
    if is_ip:
        critical_flags["Недостоверные сведения по адресу"] = False
        critical_flags["Недостоверные сведения по директору"] = False
        critical_flags["Недостоверные сведения по учредителю"] = False
        critical_flags["Массовый директор"] = False
        critical_flags["Массовый юридический адрес"] = False
        critical_flags["Дисквалификация должностных лиц"] = False
    else:
        critical_flags["Недостоверные сведения по адресу"] = company.get("ЮрАдрес", {}).get("Недост", False)
        
        directors_unreliable = any(m.get("Недост") for m in company.get("Руковод", []))
        critical_flags["Недостоверные сведения по директору"] = directors_unreliable
        
        founders_unreliable = False
        for f in company.get("Учред", {}).get("ФЛ", []):
            if f.get("Недост"):
                founders_unreliable = True
                break
        if not founders_unreliable:
            for ur in company.get("Учред", {}).get("РосОрг", []):
                if ur.get("Недост"):
                    founders_unreliable = True
                    break
        critical_flags["Недостоверные сведения по учредителю"] = founders_unreliable
        
        mass_director = company.get("МассРуковод", False) or any(m.get("МассРуковод") for m in company.get("Руковод", []))  
        # ДЛЯ КОМПАНИЙ ИЗ РУЧНОГО СПРАВОЧНИКА МАССОВЫЙ Руководитель НЕ УЧИТЫВАЕМ (всегда False)
        if inn in MANUAL_BRANDS:
            critical_flags["Массовый директор"] = False
        elif is_largest:
            critical_flags["Массовый директор"] = False
        else:
            critical_flags["Массовый директор"] = isinstance(mass_director, list) and len(mass_director) > 0
        
        mass_addr = company.get("ЮрАдрес", {}).get("МассАдрес")
        # ДЛЯ КОМПАНИЙ ИЗ РУЧНОГО СПРАВОЧНИКА МАССОВЫЙ АДРЕС НЕ УЧИТЫВАЕМ (всегда False)
        if inn in MANUAL_BRANDS:
            critical_flags["Массовый юридический адрес"] = False
        elif is_largest:
            critical_flags["Массовый юридический адрес"] = False
        ###### РУчное исключение проверки
        else:
            #critical_flags["Массовый юридический адрес"] = isinstance(mass_addr, list) and len(mass_addr) > 0
            critical_flags["Массовый юридический адрес"] = False

        disqualified = any(m.get("ДисквЛицо") for m in company.get("Руковод", []))
        critical_flags["Дисквалификация должностных лиц"] = disqualified
    
    # Общие для всех
    critical_flags["Включение в реестр недобросовестных поставщиков"] = company.get("НедобПост", False)
    
    # Существенные исполнительные производства (относительная величина)
    charter_capital = safe_float(company.get("УстКап", {}).get("Сумма")) or 0
    total_debt = sum(e.get("СумДолг", 0) for e in enforcements)
    if charter_capital > 0:
        debt_ratio = total_debt / charter_capital
    else:
        debt_ratio = float('inf') if total_debt > 0 else 0
    critical_flags["Существенные исполнительные производства"] = debt_ratio > 0.05
    
    # Отсутствие отчётности
    has_recent = any(str(y) in finances for y in (2024, 2025)) if not is_ip else False
    if is_ip:
        no_recent = False
    elif is_largest:
        no_recent = False
    else:
        no_recent = not has_recent
    critical_flags["Отсутствие отчётности за последний год"] = no_recent
    
    # Признак "однодневной" организации
    one_day = False
    if not is_ip and has_recent and len([y for y in (2024, 2025) if str(y) in finances]) == 1:
        report_year = 2024 if '2024' in finances else 2025 if '2025' in finances else None
        if report_year:
            yr_data = finances.get(str(report_year), {})
            if isinstance(yr_data.get("1600"), dict):
                balance = safe_float(yr_data["1600"].get("СумОтч"))
            else:
                balance = safe_float(yr_data.get("1600"))
            if balance is not None and balance <= 100_000:
                one_day = True
    critical_flags["Признак 'однодневной' организации"] = one_day
    
    critical_true = [name for name, val in critical_flags.items() if val]
    status = "critical" if critical_true else "ok"
    
    # Получаем варианты названий
    name_variants = get_name_variants(company, inn, is_ip)
    
    data = {
        "company": company,
        "enforcements": enforcements,
        "finances": finances,
        "is_largest": is_largest,
        "is_ip": is_ip,
        "charter_capital": charter_capital,
        "total_enforcements_debt": total_debt,
        "has_recent_finance": has_recent,
        "name_variants": name_variants,
    }
    
    return status, critical_true, data, name_variants, is_largest, is_ip

# ======================== БЛОК 2: ДОПОЛНИТЕЛЬНЫЕ ДАННЫЕ ========================
def additional_data(data: Dict[str, Any]) -> Dict[str, Any]:
    print("Блок 2: сбор дополнительных данных...")
    inn = data["company"].get("ИНН")
    
    if "inspections" not in data:
        insp_resp = fetch_data("inspections", {"inn": inn, "limit": 100})
        data["inspections"] = insp_resp.get("data", {}).get("Записи", []) if insp_resp else []
    
    if "legal_cases" not in data:
        legal_resp = fetch_data("legal-cases", {"inn": inn, "role": "defendant", "limit": 100})
        data["legal_cases"] = legal_resp.get("data", {}).get("Записи", []) if legal_resp else []
    
    if "contracts" not in data:
        contr_resp = fetch_data("contracts", {"inn": inn, "role": "supplier", "law": "44", "limit": 100})
        data["contracts"] = contr_resp.get("data", {}).get("Записи", []) if contr_resp else []
    
    return data

# ======================== БЛОК 3: РАСЧЁТ СКОРА ========================
def calculate_score(data: Dict[str, Any]) -> Tuple[Dict[str, float], List[Dict[str, str]], pd.DataFrame, Dict]:
    print("Блок 3: расчёт скора...")
    
    company = data.get("company", {})
    inspections = data.get("inspections", [])
    enforcements = data.get("enforcements", [])
    legal_cases = data.get("legal_cases", [])
    contracts = data.get("contracts", [])
    finances = data.get("finances", {})
    is_largest = data.get("is_largest", False)
    is_ip = data.get("is_ip", False)
    has_recent_finance = data.get("has_recent_finance", False) and not is_ip
    
    if is_ip:
        has_recent_finance = False
    
    # Определение последнего года отчётности (для вывода)
    last_report_year = None
    if has_recent_finance:
        for y in (2025, 2024):
            if str(y) in finances:
                last_report_year = y
                break
    
    # --- Функции-помощники для финансов ---
    if has_recent_finance:
        available_years = sorted([int(y) for y in finances.keys() if y.isdigit() and int(y) in (2024,2025)], reverse=True)
        year_curr = available_years[0] if available_years else None
        year_prev = available_years[1] if len(available_years) > 1 else None
    else:
        year_curr = year_prev = None
    
    def get_val(year, code):
        if year is None:
            return None
        yr_data = finances.get(str(year), {})
        cell = yr_data.get(str(code))
        if isinstance(cell, dict):
            return safe_float(cell.get("СумОтч"))
        return safe_float(cell)
    
    def get_prev_bal(year, code):
        if year is None:
            return None
        yr_data = finances.get(str(year), {})
        cell = yr_data.get(str(code))
        if isinstance(cell, dict):
            return safe_float(cell.get("СумПрдщ"))
        return None
    
    def get_prev_pl(year, code):
        if year is None:
            return None
        yr_data = finances.get(str(year), {})
        cell = yr_data.get(str(code))
        if isinstance(cell, dict):
            return safe_float(cell.get("СумПред"))
        return None
    
    # --- Детальные результаты ---
    details = []
    high_risk_params = []
    
    # 1. Финансовое состояние
    fin_scores = []
    
    fin_param_names = [
        "Динамика выручки", "Валовая рентабельность", "Динамика чистой прибыли",
        "Динамика собственного капитала", "Коэффициент финансовой независимости",
        "Чистый оборотный капитал", "Доля постоянных активов", "Рентабельность собственного капитала (ROE)",
        "Коэффициент краткосрочной ликвидности", "Оборачиваемость оборотного капитала",
        "Обеспеченность СОС", "Динамика операционного цикла", "Оборачиваемость дебиторской задолженности",
        "Оборачиваемость запасов", "Оборачиваемость кредиторской задолженности",
        "Рентабельность продаж", "Общий долг / EBIT", "Коэффициент общей ликвидности",
        "Рентабельность по чистой прибыли", "ЧП / дельта нераспред. прибыли",
        "EBIT / проценты", "Динамика EBIT", "Доля финансовых вложений"
    ]
    
    if is_ip:
        for param in fin_param_names:
            details.append((param, "не применимо для ИП", 2))
            fin_scores.append(2)
        total_fin_score = sum(fin_scores)  # 46
    elif not has_recent_finance:
        for param in fin_param_names:
            details.append((param, "нет данных", 1))
            fin_scores.append(1)
        total_fin_score = sum(fin_scores)
    else:
        # --- Расчёты для юрлиц с актуальной отчётностью ---
        # Динамика выручки
        rev_curr = get_val(year_curr, 2110)
        rev_prev = get_prev_pl(year_curr, 2110) or get_val(year_prev, 2110)
        if rev_curr is not None and rev_prev and rev_prev != 0:
            rev_growth = (rev_curr - rev_prev) / rev_prev * 100
            if rev_growth >= -15:
                score = 5
            elif rev_growth >= -25:
                score = 2
            else:
                score = 0
            val = f"{rev_growth:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Динамика выручки", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Динамика выручки", "значение": rev_growth if 'rev_growth' in locals() else None})
        
        # Валовая рентабельность
        gross = get_val(year_curr, 2100)
        if rev_curr and rev_curr != 0 and gross is not None:
            gross_margin = gross / rev_curr * 100
            if gross_margin > 20:
                score = 5
            elif gross_margin > 0:
                score = 2
            else:
                score = 0
            val = f"{gross_margin:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Валовая рентабельность", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Валовая рентабельность", "значение": gross_margin if 'gross_margin' in locals() else None})
        
        # Динамика чистой прибыли
        np_curr = get_val(year_curr, 2400)
        np_prev = get_prev_pl(year_curr, 2400) or get_val(year_prev, 2400)
        if np_curr is not None and np_prev and np_prev != 0:
            np_growth = (np_curr - np_prev) / np_prev * 100
            if np_growth >= -15:
                score = 5
            elif np_growth >= -25:
                score = 2
            else:
                score = 0
            val = f"{np_growth:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Динамика чистой прибыли", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Динамика чистой прибыли", "значение": np_growth if 'np_growth' in locals() else None})
        
        # Динамика собственного капитала
        eq_curr = get_val(year_curr, 1300)
        eq_prev = get_prev_bal(year_curr, 1300) or get_val(year_prev, 1300)
        if eq_curr is not None and eq_prev and eq_prev != 0:
            eq_growth = (eq_curr - eq_prev) / eq_prev * 100
            if eq_growth >= -15:
                score = 5
            elif eq_growth >= -25:
                score = 3
            else:
                score = 0
            val = f"{eq_growth:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Динамика собственного капитала", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Динамика собственного капитала", "значение": eq_growth if 'eq_growth' in locals() else None})
        
        # Коэффициент финансовой независимости
        assets_curr = get_val(year_curr, 1600)
        if eq_curr and assets_curr and assets_curr != 0:
            fin_indep = eq_curr / assets_curr
            if fin_indep < 0.3:
                score = 0
            elif fin_indep <= 0.7:
                score = 2
            else:
                score = 4
            val = f"{fin_indep:.3f}"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Коэффициент финансовой независимости", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Коэффициент финансовой независимости", "значение": fin_indep if 'fin_indep' in locals() else None})
        
        # Чистый оборотный капитал
        curr_assets = get_val(year_curr, 1200)
        curr_liab = get_val(year_curr, 1500)
        if curr_assets is not None and curr_liab is not None:
            nwc = curr_assets - curr_liab
            score = 4 if nwc >= 0 else 0
            val = f"{nwc:,.0f} руб."
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Чистый оборотный капитал", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Чистый оборотный капитал", "значение": nwc if 'nwc' in locals() else None})
        
        # Доля постоянных активов
        non_curr = get_val(year_curr, 1100)
        if non_curr and assets_curr and assets_curr != 0:
            non_curr_share = non_curr / assets_curr * 100
            score = 4 if non_curr_share < 10 else 5
            val = f"{non_curr_share:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Доля постоянных активов", val, score))
        
        # ROE
        if eq_curr and np_curr and eq_curr != 0:
            roe = np_curr / eq_curr * 100
            if roe < 10:
                score = 0
            elif roe <= 30:
                score = 2
            else:
                score = 4
            val = f"{roe:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Рентабельность собственного капитала (ROE)", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "ROE", "значение": roe if 'roe' in locals() else None})
        
        # Коэффициент краткосрочной ликвидности
        if curr_assets and curr_liab and curr_liab != 0:
            cr = curr_assets / curr_liab
            if cr < 0.7:
                score = 0
            elif cr <= 1.0:
                score = 2
            else:
                score = 4
            val = f"{cr:.2f}"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Коэффициент краткосрочной ликвидности", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Коэффициент краткосрочной ликвидности", "значение": cr if 'cr' in locals() else None})
        
        # Оборачиваемость оборотного капитала (дни)
        if curr_assets and rev_curr and rev_curr != 0:
            turnover_days = (curr_assets / rev_curr) * 365
            if turnover_days <= 90:
                score = 0
            elif turnover_days <= 180:
                score = 2
            else:
                score = 4
            val = f"{turnover_days:.0f} дн."
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Оборачиваемость оборотного капитала", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Оборачиваемость оборотного капитала", "значение": turnover_days if 'turnover_days' in locals() else None})
        
        # Обеспеченность собственными оборотными средствами
        if eq_curr is not None and non_curr is not None and curr_assets:
            sos = eq_curr - non_curr
            if curr_assets != 0:
                sos_ratio = sos / curr_assets * 100
                if sos_ratio < 10 or sos_ratio < 0:
                    score = 0
                elif sos_ratio <= 30:
                    score = 2
                else:
                    score = 4
                val = f"{sos_ratio:.1f}%"
            else:
                score = 1
                val = "нет данных"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Обеспеченность СОС", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Обеспеченность СОС", "значение": sos_ratio if 'sos_ratio' in locals() else None})
        
        # Динамика операционного цикла
        inv_curr = get_val(year_curr, 1210)
        rec_curr = get_val(year_curr, 1230)
        inv_prev = get_prev_bal(year_curr, 1210) or get_val(year_prev, 1210)
        rec_prev = get_prev_bal(year_curr, 1230) or get_val(year_prev, 1230)
        
        if rev_curr and rev_curr != 0:
            inv_turn_curr = (inv_curr / rev_curr * 365) if inv_curr else None
            rec_turn_curr = (rec_curr / rev_curr * 365) if rec_curr else None
        else:
            inv_turn_curr = rec_turn_curr = None
        
        if year_prev and get_val(year_prev, 2110):
            rev_prev_val = get_val(year_prev, 2110)
            inv_turn_prev = (inv_prev / rev_prev_val * 365) if inv_prev else None
            rec_turn_prev = (rec_prev / rev_prev_val * 365) if rec_prev else None
        else:
            inv_turn_prev = rec_turn_prev = None
        
        if inv_turn_curr is not None and rec_turn_curr is not None and inv_turn_prev is not None and rec_turn_prev is not None:
            op_cycle_curr = inv_turn_curr + rec_turn_curr
            op_cycle_prev = inv_turn_prev + rec_turn_prev
            if op_cycle_prev != 0:
                op_growth = (op_cycle_curr - op_cycle_prev) / op_cycle_prev * 100
                if op_growth > 30:
                    score = 0
                elif op_growth >= 20:
                    score = 2
                else:
                    score = 4
                val = f"{op_growth:.1f}%"
            else:
                score = 1
                val = "нет данных"
        else:
            score = 1
            val = "нет данных"
            op_growth = None
        fin_scores.append(score)
        details.append(("Динамика операционного цикла", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Динамика операционного цикла", "значение": op_growth})
        
        # Оборачиваемость дебиторской задолженности
        if rec_curr and rev_curr and rev_curr != 0:
            rec_days = rec_curr / rev_curr * 365
            if rec_days <= 60:
                score = 4
            elif rec_days <= 90:
                score = 2
            else:
                score = 0
            val = f"{rec_days:.0f} дн."
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Оборачиваемость дебиторской задолженности", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Оборачиваемость дебиторки", "значение": rec_days if 'rec_days' in locals() else None})
        
        # Оборачиваемость запасов
        cost_curr = get_val(year_curr, 2120)
        if inv_curr and cost_curr and cost_curr != 0:
            inv_days = inv_curr / cost_curr * 365
            if inv_days <= 90:
                score = 4
            elif inv_days <= 180:
                score = 2
            else:
                score = 0
            val = f"{inv_days:.0f} дн."
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Оборачиваемость запасов", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Оборачиваемость запасов", "значение": inv_days if 'inv_days' in locals() else None})
        
        # Оборачиваемость кредиторской задолженности
        pay_curr = get_val(year_curr, 1520)
        if pay_curr and cost_curr and cost_curr != 0:
            pay_days = pay_curr / cost_curr * 365
            if pay_days <= 60:
                score = 4
            elif pay_days <= 90:
                score = 2
            else:
                score = 0
            val = f"{pay_days:.0f} дн."
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Оборачиваемость кредиторской задолженности", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Оборачиваемость кредиторки", "значение": pay_days if 'pay_days' in locals() else None})
        
        # Рентабельность продаж
        profit_sales = get_val(year_curr, 2200)
        if profit_sales and rev_curr and rev_curr != 0:
            sales_margin = profit_sales / rev_curr * 100
            if sales_margin > 25:
                score = 4
            elif sales_margin >= 8:
                score = 2
            else:
                score = 0
            val = f"{sales_margin:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Рентабельность продаж", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Рентабельность продаж", "значение": sales_margin if 'sales_margin' in locals() else None})
        
        # Общий долг / EBIT
        long_loan = get_val(year_curr, 1410)
        short_loan = get_val(year_curr, 1510)
        ebit = get_val(year_curr, 2300)
        if long_loan is not None and short_loan is not None and ebit and ebit != 0:
            debt = long_loan + short_loan
            debt_ebit = debt / ebit
            if debt_ebit < 3.5:
                score = 5
            elif debt_ebit <= 4.5:
                score = 3
            else:
                score = 0
            val = f"{debt_ebit:.2f}"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Общий долг / EBIT", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Общий долг/EBIT", "значение": debt_ebit if 'debt_ebit' in locals() else None})
        
        # Коэффициент общей (текущей) ликвидности
        if curr_assets and curr_liab and curr_liab != 0:
            cl = curr_assets / curr_liab
            if cl < 1:
                score = 0
            elif cl <= 1.5:
                score = 2
            else:
                score = 4
            val = f"{cl:.2f}"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Коэффициент общей ликвидности", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Коэффициент общей ликвидности", "значение": cl if 'cl' in locals() else None})
        
        # Рентабельность по чистой прибыли
        if np_curr and rev_curr and rev_curr != 0:
            np_margin = np_curr / rev_curr * 100
            if np_margin > 20:
                score = 4
            elif np_margin >= 6:
                score = 3
            else:
                score = 0
            val = f"{np_margin:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Рентабельность по чистой прибыли", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Рентабельность по чистой прибыли", "значение": np_margin if 'np_margin' in locals() else None})
        
        # Соотношение ЧП к дельте нераспределенной прибыли
        retained_curr = get_val(year_curr, 1370)
        retained_prev = get_prev_bal(year_curr, 1370) or get_val(year_prev, 1370)
        if np_curr and retained_curr is not None and retained_prev is not None and retained_curr != retained_prev:
            delta_retained = retained_curr - retained_prev
            if delta_retained != 0:
                ratio = np_curr / delta_retained * 100
                if ratio <= 30:
                    score = 0
                elif ratio <= 70:
                    score = 2
                else:
                    score = 4
                val = f"{ratio:.1f}%"
            else:
                score = 1
                val = "нет данных"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("ЧП / дельта нераспред. прибыли", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "ЧП/Δ retained", "значение": ratio if 'ratio' in locals() else None})
        
        # EBIT / проценты к уплате
        interest = get_val(year_curr, 2330)
        if ebit and interest and interest != 0:
            coverage = ebit / interest
            score = 5 if coverage >= 2 else 0
            val = f"{coverage:.2f}"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("EBIT / проценты", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "EBIT/проценты", "значение": coverage if 'coverage' in locals() else None})
        
        # Динамика EBIT
        ebit_prev = get_prev_pl(year_curr, 2300) or get_val(year_prev, 2300)
        if ebit and ebit_prev and ebit_prev != 0:
            ebit_growth = (ebit - ebit_prev) / ebit_prev * 100
            if ebit_growth >= -15:
                score = 5
            elif ebit_growth >= -25:
                score = 3
            else:
                score = 0
            val = f"{ebit_growth:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Динамика EBIT", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Динамика EBIT", "значение": ebit_growth if 'ebit_growth' in locals() else None})
        
        # Доля финансовых вложений от ЧА
        fin_inv = get_val(year_curr, 1170)
        net_assets = get_val(year_curr, 3600)
        if fin_inv and net_assets and net_assets != 0:
            fin_share = fin_inv / net_assets * 100
            if fin_share > 30:
                score = 0
            elif fin_share >= 15:
                score = 2
            else:
                score = 4
            val = f"{fin_share:.1f}%"
        else:
            score = 1
            val = "нет данных"
        fin_scores.append(score)
        details.append(("Доля финансовых вложений", val, score))
        if score == 0:
            high_risk_params.append({"группа": "Финансовое состояние", "параметр": "Доля финвложений", "значение": fin_share if 'fin_share' in locals() else None})
        
        if is_largest:
            total_fin_score = 1
        else:
            total_fin_score = sum(fin_scores)
    
    # 2. Деловая активность и опыт
    bus_scores = []
    
    # Возраст компании
    reg_date_str = company.get("ДатаРег")
    if reg_date_str:
        try:
            reg_date = datetime.strptime(reg_date_str, "%Y-%m-%d")
            age_days = (datetime.now() - reg_date).days
            age_years = age_days / 365.25
            if age_years > 10:
                score = 40
            elif age_years >= 5:
                score = 30
            elif age_years >= 3:
                score = 20
            elif age_years >= 1:
                score = 10
            else:
                score = 0
            val = f"{age_years:.1f} лет"
        except:
            score = 0
            val = "ошибка формата"
    else:
        score = 0
        val = "нет данных"
    bus_scores.append(score)
    details.append(("Возраст компании", val, score))
    if score == 0:
        high_risk_params.append({"группа": "Деловая активность", "параметр": "Возраст компании", "значение": age_years if 'age_years' in locals() else None})
    
    # Участие в госзакупках (44-ФЗ)
    three_years_ago = datetime.now() - timedelta(days=3*365)
    completed = 0
    for c in contracts:
        exec_date_str = c.get("ДатаИсп")
        if exec_date_str:
            try:
                exec_date = datetime.strptime(exec_date_str, "%Y-%m-%d")
                if exec_date >= three_years_ago and exec_date <= datetime.now():
                    completed += 1
            except:
                pass
    if completed > 20:
        score = 15
    elif completed >= 10:
        score = 10
    elif completed > 0:
        score = 5
    else:
        score = 5
    val = str(completed)
    bus_scores.append(score)
    details.append(("Участие в госзакупках (44-ФЗ)", val, score))
    # Не добавляем в high_risk, так как 0 баллов не бывает
    
    # История проверок и соблюдение требований
    violations = False
    for insp in inspections:
        if insp.get("Наруш") and insp.get("ДатаОконч"):
            try:
                d = datetime.strptime(insp["ДатаОконч"], "%Y-%m-%d")
                if d >= three_years_ago:
                    violations = True
                    break
            except:
                pass
    if inspections:
        score = 0 if violations else 15
        val = "есть нарушения" if violations else "нет нарушений"
    else:
        score = 15
        val = "нет данных"
    bus_scores.append(score)
    details.append(("История проверок", val, score))
    if score == 0:
        high_risk_params.append({"группа": "Деловая активность", "параметр": "Нарушения в проверках", "значение": violations})
    
    # История изменений компании
    one_year_ago = datetime.now() - timedelta(days=365)
    changes = False
    for m in company.get("Руковод", []):
        if m.get("ДатаЗаписи"):
            try:
                d = datetime.strptime(m["ДатаЗаписи"], "%Y-%m-%d")
                if d >= one_year_ago:
                    changes = True
                    break
            except:
                pass
    if not changes:
        for f in company.get("Учред", {}).get("ФЛ", []):
            if f.get("ДатаЗаписи"):
                try:
                    d = datetime.strptime(f["ДатаЗаписи"], "%Y-%m-%d")
                    if d >= one_year_ago:
                        changes = True
                        break
                except:
                    pass
    if not changes:
        for ur in company.get("Учред", {}).get("РосОрг", []):
            if ur.get("ДатаЗаписи"):
                try:
                    d = datetime.strptime(ur["ДатаЗаписи"], "%Y-%m-%d")
                    if d >= one_year_ago:
                        changes = True
                        break
                except:
                    pass
    if company.get("Руковод") or company.get("Учред"):
        score = 0 if changes else 15
        val = "были изменения" if changes else "нет изменений"
    else:
        score = 0
        val = "нет данных"
    bus_scores.append(score)
    details.append(("История изменений компании", val, score))
    if score == 0:
        high_risk_params.append({"группа": "Деловая активность", "параметр": "Изменения за 12 мес", "значение": changes})
    
    total_bus_score = sum(bus_scores)
    
    # 3. Правовые риски
    legal_scores = []
    
    # Арбитражные суды (ответчик)
    num_legal = len(legal_cases)
    if legal_cases:
        if num_legal > 10:
            score = 0
        elif num_legal >= 5:
            score = 10
        elif num_legal >= 1:
            score = 15
        else:
            score = 25
        val = str(num_legal)
    else:
        score = 25
        val = "нет данных"
    legal_scores.append(score)
    details.append(("Арбитражные дела (ответчик)", val, score))
    if score == 0:
        high_risk_params.append({"группа": "Правовые риски", "параметр": "Арбитражные дела", "значение": num_legal})
    
    # Исполнительные производства (количество)
    num_enforce = len(enforcements)
    if enforcements:
        if num_enforce > 10:
            score = 0
        elif num_enforce >= 5:
            score = 10
        elif num_enforce >= 1:
            score = 15
        else:
            score = 25
        val = str(num_enforce)
    else:
        score = 25
        val = "нет данных"
    legal_scores.append(score)
    details.append(("Исполнительные производства", val, score))
    if score == 0:
        high_risk_params.append({"группа": "Правовые риски", "параметр": "Исполнительные производства", "значение": num_enforce})
    
    # Налоговые задолженности
    tax_debt = safe_float(company.get("Налоги", {}).get("СумНедоим"))
    if tax_debt is not None:
        if tax_debt > 100_000:
            score = 0
        elif tax_debt >= 50_000:
            score = 10
        elif tax_debt >= 1_000:
            score = 15
        else:
            score = 25
        val = f"{tax_debt:,.0f} руб."
    else:
        score = 25
        val = "нет данных"
    legal_scores.append(score)
    details.append(("Налоговая задолженность", val, score))
    if score == 0:
        high_risk_params.append({"группа": "Правовые риски", "параметр": "Налоговая задолженность", "значение": tax_debt})
    
    # Лицензии и разрешения
    licensed_okved = {
        '11.01', '11.02', '11.03', '11.04', '11.05', '11.06', '11.07',
        '46.34', '47.11', '47.25', '21.10', '21.20', '46.46', '47.73',
        '86.10', '86.21', '86.22', '86.23', '86.90', '85.11', '85.12',
        '85.13', '85.14', '85.21', '85.22', '85.23', '85.30', '85.41',
        '85.42', '49.10', '49.20', '49.31', '49.39', '49.41', '50.10',
        '50.20', '51.10', '51.21', '80.10', '80.20', '80.30', '61.10',
        '61.20', '61.30', '61.90', '60.10', '60.20', '26.30', '26.20',
        '72.19', '71.12', '71.20', '84.13', '05.10', '05.20', '06.10',
        '06.20', '07.10', '08.11', '19.20', '20.13', '35.30', '49.50',
        '38.11', '38.12', '38.21', '38.22', '38.32', '43.21', '33.13'
    }
    okved_main = company.get("ОКВЭД", {}).get("Код")
    licenses = company.get("Лиценз", [])
    
    need_license = okved_main in licensed_okved
    
    now = datetime.now()
    has_valid_license = False
    for lic in licenses:
        expiry = lic.get("ДатаОконч")
        if expiry:
            try:
                exp_date = datetime.strptime(expiry, "%Y-%m-%d")
                if exp_date > now:
                    has_valid_license = True
                    break
            except:
                pass
    
    if need_license and not has_valid_license:
        score = 0
        val = "лицензия отсутствует или просрочена"
    else:
        score = 25
        val = "лицензия есть или не требуется"
    legal_scores.append(score)
    details.append(("Лицензии", val, score))
    if score == 0:
        high_risk_params.append({"группа": "Правовые риски", "параметр": "Лицензии", "значение": "отсутствует необходимая лицензия"})
    
    total_legal_score = sum(legal_scores)
    
    groups_scores = {
        "Финансовое состояние": total_fin_score,
        "Деловая активность и опыт": total_bus_score,
        "Правовые риски": total_legal_score
    }
    
    details_df = pd.DataFrame(details, columns=["Параметр", "Значение", "Балл"])
    
    extra_info = {
        "inn": company.get("ИНН"),
        "kpp": company.get("КПП") if not is_ip else None,
        "okved_main": okved_main,
        "registration_date": company.get("ДатаРег"),
        "last_report_year": last_report_year,
    }
    
    return groups_scores, high_risk_params, details_df, extra_info