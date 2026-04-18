import sqlite3
import uuid
import os
import traceback
from flask import Flask, request, jsonify, render_template, make_response
from dotenv import load_dotenv

# Загружаем переменные окружения ДО импорта aggregator
load_dotenv()

# Импортируем основную функцию
from aggregator import check_company

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-for-testing')

# Инициализация базы данных для хранения количества проверок
def init_db():
    conn = sqlite3.connect('reports.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS report_counts
                 (session_id TEXT PRIMARY KEY, count INTEGER)''')
    conn.commit()
    conn.close()

init_db()

def get_session_id():
    """Получить session_id из куки или создать новый."""
    session_id = request.cookies.get('riskguard_session')
    if not session_id:
        session_id = str(uuid.uuid4())
    return session_id

@app.route('/')
def index():
    session_id = get_session_id()
    resp = make_response(render_template('index.html'))
    if not request.cookies.get('riskguard_session'):
        resp.set_cookie('riskguard_session', session_id, max_age=30*24*60*60)  # 30 дней
    return resp

@app.route('/check', methods=['POST'])
def check():
    data = request.get_json()
    inn = data.get('inn')
    if not inn:
        return jsonify({'error': 'ИНН не указан'}), 400

    session_id = get_session_id()

    # Работа со счётчиком в БД
    conn = sqlite3.connect('reports.db')
    c = conn.cursor()
    c.execute("SELECT count FROM report_counts WHERE session_id = ?", (session_id,))
    row = c.fetchone()
    if row:
        report_number = row[0] + 1
        c.execute("UPDATE report_counts SET count = ? WHERE session_id = ?", (report_number, session_id))
    else:
        report_number = 1
        c.execute("INSERT INTO report_counts (session_id, count) VALUES (?, ?)", (session_id, report_number))
    conn.commit()
    conn.close()

    try:
        result = check_company(inn, session_id, report_number)
    except Exception as e:
        # Выводим полный стек ошибки в консоль для отладки
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

    # Устанавливаем куку, если её ещё нет
    resp = make_response(jsonify(result))
    if not request.cookies.get('riskguard_session'):
        resp.set_cookie('riskguard_session', session_id, max_age=30*24*60*60)
    return resp

@app.route('/banner-action', methods=['POST'])
def banner_action():
    data = request.get_json()
    action = data.get('action')
    session_id = get_session_id()
    report_number = data.get('report_number')

    from aggregator import send_banner_log
    send_banner_log(session_id, action, report_number)

    return jsonify({'status': 'ok'})

@app.route('/visit', methods=['POST'])
def visit():
    session_id = get_session_id()
    referrer = request.referrer or 'direct'
    user_agent = request.headers.get('User-Agent', '')

    from aggregator import send_visit_log
    send_visit_log(session_id, referrer, user_agent)

    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(debug=True)