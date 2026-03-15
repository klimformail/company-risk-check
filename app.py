from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
import os

# Загружаем переменные из .env до всех остальных импортов
load_dotenv()

# Теперь можно импортировать модули, которые используют переменные окружения
from aggregator import check_company

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/check', methods=['POST'])
def check():
    data = request.get_json()
    inn = data.get('inn')
    if not inn:
        return jsonify({'error': 'ИНН не указан'}), 400
    try:
        result = check_company(inn)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)