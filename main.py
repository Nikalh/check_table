# -*- coding: utf-8 -*-
import asyncio
import os
import datetime
import sys
import html
import gc  # Добавляем сборщик мусора

print("🚀 Starting Telegram Excel Bot on Render (Optimized)...")

# Импорты
import pandas as pd
from openpyxl import load_workbook
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")

if not TOKEN:
    print("❌ TELEGRAM_TOKEN not found!")
    sys.exit(1)

print("✅ Telegram token loaded successfully")

bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()
user_data = {}

# --- Оптимизированная проверка Excel ---
async def check_excel(user_id, notify_today=True, week_summary=False):
    print(f"🔍 Checking Excel for user {user_id}")
    
    file_path = await download_file(user_id)
    if not file_path:
        return
        
    try:
        # Оптимизация: загружаем только нужные данные
        wb = load_workbook(file_path, data_only=True, read_only=True)  # read_only для экономии памяти
    except Exception as e:
        await bot.send_message(user_id, f"❌ Ошибка открытия файла: {e}")
        return
        
    sheet_name = "Согласование документации"
    if sheet_name not in wb.sheetnames:
        await bot.send_message(user_id, f"❌ Лист '{sheet_name}' не найден")
        wb.close()  # Закрываем файл
        return
        
    ws = wb[sheet_name]
    today = datetime.date.today()
    overdue_items = []

    # Читаем заголовки
    headers = []
    for cell in ws[1]:
        headers.append(cell.value)
    
    col_map = {name: idx for idx, name in enumerate(headers) if name}

    days_limit = user_data[user_id].get("days", 30)

    # Обрабатываем строки с оптимизацией памяти
    for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
        try:
            # Только нужные колонки
            obj = row[col_map["Объект"]].value if "Объект" in col_map else ""
            task = row[col_map["Сооружение"]].value if "Сооружение" in col_map else ""
            resp = row[col_map["Ответственный"]].value if "Ответственный" in col_map else ""
            subject = row[col_map["Предмет письма"]].value if "Предмет письма" in col_map else ""
            
            pg_cell = row[col_map["Срок от ПГ"]] if "Срок от ПГ" in col_map else None
            cc_cell = row[col_map["Направил в ЦЦО"]] if "Направил в ЦЦО" in col_map else None

            date_pg = parse_date(pg_cell.value if pg_cell else None)
            date_cc = parse_date(cc_cell.value if cc_cell else None)

            # Проверяем только НЕ выполненные задачи
            if pg_cell and date_pg and date_pg <= today and not is_done(pg_cell):
                overdue_items.append(
                    f"📍 <b>{html.escape(str(obj))}</b>\n"
                    f"📝 {html.escape(str(task))}</b>\n"
                    f"👤 {html.escape(str(resp))}</b>\n"
                    f"✉ {html.escape(str(subject))}</b>\n"
                    f"⏰ Срок от ПГ: {html.escape(str(date_pg))}"
                )
                
            if cc_cell and date_cc and (today - date_cc).days >= days_limit and not is_done(cc_cell):
                overdue_items.append(
                    f"📍 <b>{html.escape(str(obj))}</b>\n"
                    f"📝 {html.escape(str(task))}</b>\n"
                    f"👤 {html.escape(str(resp))}</b>\n"
                    f"✉ {html.escape(str(subject))}</b>\n"
                    f"⏰ Направлено в ЦЦО: {html.escape(str(date_cc))} ({html.escape(str((today - date_cc).days))} дней)"
                )
                
        except Exception as e:
            print(f"Error in row {row_idx}: {e}")
            continue

    # Закрываем workbook для освобождения памяти
    wb.close()

    if overdue_items:
        header = f"⚠️ {len(overdue_items)} Просроченные задачи на сегодня {today}:\n\n" if notify_today else "📋 Сводка задач на неделю:\n\n"
        msg = header + "\n\n".join(overdue_items)
        
        chunks = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
        for chunk in chunks:
            try:
                await bot.send_message(user_id, chunk, parse_mode="HTML")
            except Exception as e:
                print(f"Error sending message: {e}")
                try:
                    clean_chunk = chunk.replace('<b>', '').replace('</b>', '')
                    await bot.send_message(user_id, clean_chunk)
                except Exception as fallback_error:
                    print(f"Fallback error: {fallback_error}")
                    
        print(f"✅ Sent {len(overdue_items)} overdue items to user {user_id}")
    elif week_summary:
        await bot.send_message(user_id, "✅ Все задачи на этой неделе в срок.")
    elif notify_today:
        await bot.send_message(user_id, "✅ На сегодня просроченных задач нет.")
    
    # Принудительная очистка памяти
    gc.collect()

# --- Команды ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.reply(
        "🤖 Бот для отслеживания задач из Excel-таблиц\n\n"
        "Отправьте мне ссылку на Excel файл или сам файл.\n"
        "Бот проверяет задачи ежедневно в 9:00 и присылает сводку по понедельникам."
    )

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    await message.reply("✅ Бот работает на PythonAnywhere!")

@dp.message(Command("test"))
async def cmd_test(message: types.Message):
    await message.reply("🔄 Тестовая проверка...")
    await check_excel(message.from_user.id, notify_today=True)

# --- Обработка сообщений ---
@dp.message()
async def handle_message(message: types.Message):
    user_id = message.from_user.id
    
    if message.text:
        text = message.text.strip()
        
        # Проверка ссылки
        if text.lower().startswith(('http://', 'https://')):
            user_data[user_id] = {"link": text, "days": 30}
            await message.reply("✅ Ссылка принята. Проверяю таблицу...")
            await check_excel(user_id)
            return
            
        # Проверка локального файла (для отладки)
        elif os.path.exists(text) and text.lower().endswith(('.xlsx', '.xls')):
            user_data[user_id] = {"path": text, "days": 30}
            await message.reply("✅ Локальный файл принят. Проверяю таблицу...")
            await check_excel(user_id)
            return
            
        else:
            # Если это не ссылка и не существующий файл, покажем инструкцию
            await message.reply(
                "📋 Отправьте мне:\n\n"
                "• **Ссылку** на Excel файл (http://...)\n"
                "• Или **сам файл** как документ\n\n"
                "Пример ссылки: https://example.com/file.xlsx"
            )
            return
        
    elif message.document and message.document.file_name:
        file_name = message.document.file_name.lower()
        if file_name.endswith(('.xlsx', '.xls')):
            file_path = f"user_{user_id}.xlsx"
            try:
                file = await bot.get_file(message.document.file_id)
                await bot.download_file(file.file_path, destination=file_path)
                user_data[user_id] = {"path": file_path, "days": 30}
                await message.reply("✅ Файл получен. Проверяю таблицу...")
                await check_excel(user_id)
            except Exception as e:
                await message.reply(f"❌ Ошибка при сохранении файла: {e}")
        else:
            await message.reply("❌ Файл должен быть Excel (.xlsx или .xls)")
        return
        
    await message.reply("❌ Отправьте Excel файл или ссылку на него")

# --- Планировщик ---
async def daily_check():
    print("🔄 Running daily check...")
    if not user_data:
        print("❌ No users found for daily check")
        return
        
    for user_id in list(user_data.keys()):
        try:
            await check_excel(user_id, notify_today=True)
        except Exception as e:
            print(f"Error in daily check for user {user_id}: {e}")

async def weekly_summary():
    print("📋 Running weekly summary...")
    if not user_data:
        print("❌ No users found for weekly summary")
        return
        
    for user_id in list(user_data.keys()):
        try:
            await check_excel(user_id, notify_today=False, week_summary=True)
        except Exception as e:
            print(f"Error in weekly summary for user {user_id}: {e}")

# --- Основная функция ---
""" async def main():
    print("✅ Bot initialized successfully")
    
    # Запускаем планировщик
    scheduler.add_job(daily_check, "cron", hour=6, minute=0, timezone="Europe/Moscow")  # 9:00 МСК
    scheduler.add_job(weekly_summary, "cron", day_of_week=0, hour=7, minute=0, timezone="Europe/Moscow")  # 10:00 МСК в воскресенье
    scheduler.start()
    
    print("⏰ Scheduler started: Daily at 09:00 MSK, Weekly on Sunday at 10:00 MSK")
    print("🤖 Bot is ready and polling...")
    
    # Запускаем бота
    await dp.start_polling(bot)
 """


# --- Основная функция ---
async def main():
    print("✅ Bot initialized successfully")
    
    # Запускаем планировщик
    scheduler.add_job(daily_check, "cron", hour=6, minute=0, timezone="Europe/Moscow")
    scheduler.add_job(weekly_summary, "cron", day_of_week=0, hour=7, minute=0, timezone="Europe/Moscow")
    scheduler.start()
    
    print("⏰ Scheduler started")
    print("🤖 Bot is ready and polling...")
    
    # Запускаем бота с перезапуском при ошибках
    restart_count = 0
    max_restarts = 10
    
    while restart_count < max_restarts:
        try:
            await dp.start_polling(bot)
        except Exception as e:
            restart_count += 1
            print(f"❌ Bot crashed (restart {restart_count}/{max_restarts}): {e}")
            print("🔄 Restarting in 30 seconds...")
            await asyncio.sleep(30)
    
    print("❌ Max restarts reached. Bot stopped.")

if __name__ == "__main__":
    asyncio.run(main())
