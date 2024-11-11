import requests
import json
import pandas as pd
from datetime import datetime
from telegram import Bot
from dotenv import load_dotenv
import os
import logging
import asyncio

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройки для Telegram бота и API Ozon
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OZON_API_TOKEN = os.getenv("OZON_API_TOKEN")
CLIENT_ID = os.getenv("CLIENT_ID")
CONVERSION_THRESHOLD_VALUE = int(os.getenv("CONVERSION_THRESHOLD_VALUE"))
user_ids = list(map(int, os.getenv("USER_IDS").split(',')))
delay = int(os.getenv("DELAY", 3600))  # Задержка по умолчанию - 1 час

# Инициализация бота
bot = Bot(token=TELEGRAM_TOKEN)

# URL для Ozon API
URL = "https://api-seller.ozon.ru/v1/analytics/data"

# Заголовки для запроса
headers = {
    "Client-Id": CLIENT_ID,
    "Api-Key": OZON_API_TOKEN,
    "Content-Type": "application/json"
}

# Имя CSV-файла для сохранения данных
file_name = "ozon_data_log.csv"
log_file = "bot_activity.log"

# Настройка логирования на русском языке с поддержкой UTF-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Проверка и создание файла, если его нет
try:
    data_log = pd.read_csv(file_name)
except FileNotFoundError:
    data_log = pd.DataFrame(columns=[
        "timestamp", "total_add_to_cart", "total_ordered_units", 
        "new_add_to_cart", "new_ordered_units"
    ])
    logging.info("CSV файл не найден, создан новый файл для логирования данных.")

# Асинхронная функция для выполнения запроса и записи данных
async def fetch_and_log_data():
    # Установка текущей даты для date_from и date_to
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Параметры для запроса статистики с автоматическим обновлением даты
    payload = {
        "date_from": current_date,
        "date_to": current_date,
        # "date_from": "2024-11-11",
        # "date_to": "2024-11-11",
        "metrics": ["hits_tocart", "ordered_units"],
        "dimension": ["sku"],
        "filters": [],
        "limit": 100
    }

    # Запрос к API
    response = requests.post(URL, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        data = response.json()
        total_add_to_cart = data["result"]["totals"][0]
        total_ordered_units = data["result"]["totals"][1]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Рассчитываем разницу для новых добавлений в корзину и заказов
        if not data_log.empty:
            last_add_to_cart = data_log.iloc[-1]["total_add_to_cart"]
            last_ordered_units = data_log.iloc[-1]["total_ordered_units"]
            # Проверяем, чтобы разница не была отрицательной
            new_add_to_cart = max(0, total_add_to_cart - last_add_to_cart)
            new_ordered_units = max(0, total_ordered_units - last_ordered_units)
        else:
            new_add_to_cart = 0
            new_ordered_units = 0

        # Вычисление конверсии за час и за день
        conversion_rate_hour = (new_ordered_units / new_add_to_cart) * 100 if new_add_to_cart > 0 else 0
        conversion_rate_day = (total_ordered_units / total_add_to_cart) * 100 if total_add_to_cart > 0 else 0

        # Формирование сообщения с общими данными
        message = (
            f"Дата и время: {timestamp}\n"
            "\n"
            f"Общее добавление в корзину: {total_add_to_cart}\n"
            f"Общее количество заказов: {total_ordered_units}\n"
            "\n"
            f"Новые добавления в корзину: {new_add_to_cart}\n"
            f"Новые заказы: {new_ordered_units}\n"
            "\n"
            f"Конверсия за последний час: {conversion_rate_hour:.2f}%\n"
            f"Конверсия за последний день: {conversion_rate_day:.2f}%"
        )

        # Проверка порога конверсии
        if conversion_rate_day < CONVERSION_THRESHOLD_VALUE:
            message = f"❌ОБЩАЯ КОНВЕРСИЯ МЕНЬШЕ {CONVERSION_THRESHOLD_VALUE}%❌\n\n" + message

        # Добавляем разбивку по каждому товару, исключая товары с нулевыми значениями
        message += "\n\nПодробная информация по каждому товару:\n"
        for item in data["result"]["data"]:
            sku_id = item["dimensions"][0]["id"]
            sku_name = item["dimensions"][0]["name"]
            hits_tocart = item["metrics"][0]
            ordered_units = item["metrics"][1]

            # Пропускаем товары с нулевыми добавлениями в корзину и заказами
            if hits_tocart == 0 and ordered_units == 0:
                continue

            # Вычисление конверсии для каждого товара
            sku_conversion_rate = (ordered_units / hits_tocart) * 100 if hits_tocart > 0 else 0

            # Проверка конверсии для каждого товара относительно порога
            if sku_conversion_rate >= CONVERSION_THRESHOLD_VALUE:
                conversion_status = f"✅ Конверсия выше ({CONVERSION_THRESHOLD_VALUE}%) ✅"
            else:
                conversion_status = f"❌ Конверсия ниже ({CONVERSION_THRESHOLD_VALUE}%) ❌"

            # Добавление информации по товару в сообщение
            message += (
                f"\nТовар: {sku_name}\n"
                f"Артикул (ID): {sku_id}\n"
                f"Добавлено в корзину: {hits_tocart}\n"
                f"Заказано: {ordered_units}\n"
                f"Конверсия: {sku_conversion_rate:.2f}% - {conversion_status}\n"
            )

        # Асинхронная отправка сообщения всем пользователям
        for user_id in user_ids:
            try:
                await bot.send_message(chat_id=user_id, text=message)
                logging.info(f"Уведомление отправлено пользователю {user_id}")
            except Exception as e:
                logging.error(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")

        # Логирование данных в CSV
        data_log.loc[len(data_log)] = [timestamp, total_add_to_cart, total_ordered_units, new_add_to_cart, new_ordered_units]
        data_log.to_csv(file_name, index=False)
        logging.info("Данные успешно записаны в CSV файл.")
    else:
        logging.error(f"Ошибка запроса к API: {response.status_code} - {response.json()}")



# Основная асинхронная функция для выполнения отправки с задержкой
async def main():
    try:
        while True:
            await fetch_and_log_data()
            logging.info(f"Следующая отправка через {delay} секунд.")
            await asyncio.sleep(delay)
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем.")
    except Exception as e:
        logging.critical(f"Критическая ошибка в работе бота: {e}")

# Запуск асинхронного цикла
if __name__ == "__main__":
    asyncio.run(main())
