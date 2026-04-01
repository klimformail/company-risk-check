from dotenv import load_dotenv

import os

import json

from datetime import datetime

from gigachat import GigaChat

import gigachat.context

from gigachat.models import Chat, Messages, MessagesRole

# import requests

import config



class LLM_Classifier():

    def __init__(self):

        load_dotenv()  # загружает данные из .env

        # self.g_chat_auth_key = os.getenv('GCH_AUTH_KEY')

    

    def get_answers(self, quests: list, system_prompt: str, auth_key: str, headers: dict):

        '''

        Функция для получения ответа от модели GigaChat-2

        quests - список, состоящий из словарей. Каждый словарь соответствует

        одному пользовательскому отзыву и содержит текстовый отзыв в поле "review";

        system_prompt - системный промпт (общая инструкция по обработке данных);

        auth_key - ключ авторизации для api Gigachat;

        headers - словарь, содержащий произвольный текстовый идентификатор сессии

        в поле "X-Session-ID" для сохранения контекста и кэширования токенов цепочки запросов

        '''

        answers = [] # сюда сохраняем ответы модели со всеми служебными полями

        with GigaChat(credentials=auth_key,

                        model='GigaChat-2',

                        verify_ssl_certs=False

                        ) as giga:

            # настройка для кэширования токенов цепочки запросов

            gigachat.context.session_id_cvar.set(headers.get("X-Session-ID"))

            

            for quest in quests:

                if isinstance(quest, dict):

                    review_text = quest.get("review")  # получаем текст отзыва

                    if review_text:

                        model_answer = giga.chat(Chat(messages=[

                            Messages(role=MessagesRole.SYSTEM, content=system_prompt),

                            Messages(role=MessagesRole.USER, content=review_text)

                            ])

                        )
                        print(f"Ответ модели: {model_answer.choices[0].message.content}")
                        answers.append(model_answer)

                    else:

                        answers.append(None)

                else:

                    answers.append(None)

        return answers

        

    def enrich_reviews(self, quests: list, answers: list) -> list:

        '''

        Дополняет каждый словарь в quests полями из ответов GigaChat

        quests - список, состоящий из словарей. Каждый словарь соответствует

        одному пользовательскому отзыву и содержит текстовый отзыв в поле "review";

        answers - список из ответов модели со всеми служебными полями;

        порядок следования вопросов в quests и ответов в answers совпадает

        '''

        for quest, answer in zip(quests, answers):

            # проверяем, что есть ответ и текст отзыва

            if answer and quest.get("review"):

                # текст ответа модели:  

                quest["content"] = answer.choices[0].message.content

                quest["timestamp"] = answer.created

                # израсходовано токенов за вычетом кэшированных:

                quest["total_tokens"] = answer.usage.total_tokens

                # количество кэшированных токенов

                quest["precached_prompt_tokens"] = answer.usage.precached_prompt_tokens

        

        return quests



    def save_to_json(self, processed_items: list, filename: str = None):

        '''

        Сохраняет в файл json результаты работы модели

        processed_items - результаты работы модели:

        запросы, ответы, timestamp, статистику по токенам;

        filename - путь для сохранения файла

        '''

        if filename is None:

            filename = f"reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        

        try:

            with open(filename, 'w', encoding='utf-8') as f:

                json.dump(processed_items, f, ensure_ascii=False, indent=2)

            

            print(f"Сохранено {len(processed_items)} записей в {filename}")

            return filename

            

        except PermissionError:

            print(f"Ошибка: нет прав на запись в файл {filename}")

            alt_filename = f"reviews_fallback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

            try:

                with open(alt_filename, 'w', encoding='utf-8') as f:

                    json.dump(processed_items, f, ensure_ascii=False, indent=2)

                print(f"Сохранено в {alt_filename}")

                return alt_filename

            except:

                print("Ошибка: не удалось сохранить файл")

                return None

                

        except json.JSONEncodeError as e:

            print(f"Ошибка сериализации JSON: {e}")

            return None

            

        except Exception as e:

            print(f"Неожиданная ошибка при сохранении: {e}")

            return None

        

    def load_from_json(self, filename: str):

        """

        Загружает данные из JSON файла и возвращает список quests

        """

        if not os.path.exists(filename):

            print(f"Ошибка: файл {filename} не найден")

            return None

        

        try:

            with open(filename, 'r', encoding='utf-8') as f:

                quests = json.load(f)

            

            print(f"Загружено {len(quests)} записей из {filename}")

            return quests

            

        except json.JSONDecodeError as e:

            print(f"Ошибка чтения JSON: {e}")

            return None

        except Exception as e:

            print(f"Неожиданная ошибка при загрузке: {e}")

            return None



    def parse_sentiment(self, model_response: str):
        if not model_response or not isinstance(model_response, str):
            return None
        clean_response = model_response.replace(' ', '').replace('\n', '')
        result = []
        i = 0
        while i < len(clean_response) - 2:
            code = clean_response[i]
            if code in config.ASPECT_CODES:
                sentiment_code = clean_response[i+1]
                confidence_code = clean_response[i+2]
                if (sentiment_code in config.SENTIMENT_VALUES and
                    confidence_code in config.CONFIDENCE_VALUES):
                    result.append({
                        'aspect': config.ASPECT_CODES[code],
                        'sentiment_value': config.SENTIMENT_VALUES[sentiment_code],
                        'confidence_value': config.CONFIDENCE_VALUES[confidence_code]
                    })
                    i += 3
                    continue
            i += 1
        return result if result else None



    def sentiment_estimate_simple(self, parsing_result: list):
        if not parsing_result or not isinstance(parsing_result, list):
            return None
        result = {}
        for asp in parsing_result:
            if not isinstance(asp, dict):
                continue
            if asp.get('sentiment_value') is not None and asp.get('confidence_value') is not None:
                sentiment = asp['sentiment_value'] * asp['confidence_value']
            else:
                sentiment = None
            result[asp.get('aspect', 'Missed')] = sentiment
        return result if result else None



    def simple_output(self, preprocessed_quests: list):

        '''

        Для списка ответов модели, 

        где каждый ответ в отдельном словаре (значение по ключу 'content'),

        формирует список с оценками аспектов (оценка на каждый отзыв без агрегации). 

        Элементы идут в том же порядке, что и во входящем списке ;

        preprocessed_quests - результат выполнения функции enrich_reviews

        '''

        if not preprocessed_quests or not isinstance(preprocessed_quests, list):

            return None

        result = []

        for r in preprocessed_quests:

            temp_dict = self.sentiment_estimate_simple(

                self.parse_sentiment(model_response=r.get('content'))

            )

            result.append(temp_dict)

        return result





    def aggregated_output(self, preprocessed_quests: list, business_id_key: str = config.BUSINESS_ID_FIELD):
        if not preprocessed_quests or not isinstance(preprocessed_quests, list):
            return None
        accumulation_dict = {}
        for ex in preprocessed_quests:
            if not ex.get(business_id_key):
                continue
            b_id = str(ex[business_id_key])
            parsed = self.parse_sentiment(ex.get('content'))
            if not parsed:
                continue
            response_dict = self.sentiment_estimate_simple(parsed)
            if not response_dict or not isinstance(response_dict, dict):
                continue
            # Инициализация накопителя для бизнеса
            if b_id not in accumulation_dict:
                accumulation_dict[b_id] = {}
            for asp, val in response_dict.items():
                acc_key = f"{asp}_acc_value"
                cnt_key = f"{asp}_count"
                if acc_key not in accumulation_dict[b_id]:
                    accumulation_dict[b_id][acc_key] = val if val is not None else 0
                    accumulation_dict[b_id][cnt_key] = 1 if val is not None else 0
                else:
                    if val is not None:
                        if accumulation_dict[b_id][acc_key] is None:
                            accumulation_dict[b_id][acc_key] = val
                        else:
                            accumulation_dict[b_id][acc_key] += val
                        accumulation_dict[b_id][cnt_key] += 1
        # Формируем итоговые средние
        result_dict = {}
        for bus_id, acc in accumulation_dict.items():
            result_dict[bus_id] = {}
            for key in acc:
                if key.endswith('_count') and acc[key] > 0:
                    aspect = key[:-6]
                    result_dict[bus_id][aspect] = acc[f"{aspect}_acc_value"] / acc[key]
        return result_dict

    

    def basic_pipeline(self, list_with_reviews, aggregate_by_business_ID=False, business_id_key=config.BUSINESS_ID_FIELD, auth_key=None):
        if not list_with_reviews or not isinstance(list_with_reviews, list):
            return None
        if not auth_key:
            auth_key = os.getenv('GCH_AUTH_KEY')
        if not auth_key:
            return None
        try:
            model_result = self.get_answers(quests=list_with_reviews,
                                            system_prompt=config.SYSTEM_PROMPT,
                                            auth_key=auth_key,
                                            headers=config.headers)
            processed_result = self.enrich_reviews(list_with_reviews, model_result)
            # self.save_to_json(processed_result)  # отключено
            if aggregate_by_business_ID:
                return self.aggregated_output(processed_result, business_id_key=business_id_key)
            else:
                return self.simple_output(processed_result)
        except Exception as e:
            print(f"Ошибка в basic_pipeline: {e}")
            import traceback
            traceback.print_exc()
            return None