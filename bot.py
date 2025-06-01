from apscheduler.schedulers.background import BackgroundScheduler #
from datetime import datetime, timedelta #
from icalendar import Calendar #
from zoneinfo import ZoneInfo #
from telebot import types #
import threading #
import sqlite3 #
import telebot #
import logging #
import random #
import json #
import os #
import re #

current_directory = os.path.dirname(os.path.abspath(__file__))
db_file = os.path.join(current_directory, 'db.db')
logs_folder = os.path.join(current_directory, 'logs')
os.makedirs(logs_folder, exist_ok=True)
log_file_path = os.path.join(logs_folder, 'bot.log')
file_path = os.path.join(current_directory, "api.json")
with open(file_path, "r") as f:data = json.load(f)
bot_token = data["api"]
bot = telebot.TeleBot(bot_token)
conn = sqlite3.connect(db_file, check_same_thread=False)

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.FileHandler(log_file_path, mode='a', encoding='utf-8')])

########################################

class Main:
    def __init__(self, bot, conn):
        # Connection to the telegram API and DB
        self.bot = bot
        self.conn = conn
        self.current_directory = os.path.dirname(os.path.abspath(__file__))
        self.cursor = self.conn.cursor()

        # Declaring variables and paths used throughout the code
        self.photo_folder = os.path.join(self.current_directory, '2b')
        self.menu_photo_path = os.path.join(self.photo_folder, 'menu.png')
        self.schedule1_photo_path = os.path.join(self.photo_folder, 'schedule1.png')
        self.schedule2_photo_path = os.path.join(self.photo_folder, 'schedule2.png')
        self.homework1_photo_path = os.path.join(self.photo_folder, 'homework1.png')
        self.homework2_photo_path = os.path.join(self.photo_folder, 'homework2.png')
        self.admin_homework_photo_path = os.path.join(self.photo_folder, 'admin_homework.png')
        self.ics_source_file_dir = os.path.join(self.current_directory, 'ics_source')
        if not os.path.exists(self.ics_source_file_dir):
            os.makedirs(self.ics_source_file_dir)
        self.ics_file1 = os.path.join(self.ics_source_file_dir, "1Studenta_grafiks_24_25-P.ics")
        self.ics_file2 = os.path.join(self.ics_source_file_dir, "2Studenta_grafiks_24_25-P.ics")
        self.last_photo_number = None

        # Calling the methods to start the bot
        self.users_table_if_not_exists()
        self.glob_commands_handler()
        self.glob_callback_handler()
        try: self.bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception: logging.exception("BOT POLLING: ")

########################################

    # Creating the table of users if it does not exist
    def users_table_if_not_exists(self):
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS users (telegram_id INTEGER UNIQUE, choice INTEGER, username TEXT, timestamp TEXT, admin_status TEXT, notif_status TEXT)''')
            self.conn.commit()

    # General method of handling all commands
    def glob_commands_handler(self):
        # /start and /menu commands handler
        @self.bot.message_handler(commands=['start', 'menu'])
        def start_message(message):
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.menu_init(message)

        # /change_group command handler
        @self.bot.message_handler(commands=['change_group'])
        def group_change_event(message):
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.ask_for_choice(message)

        # /turnon_notifications command handler
        @self.bot.message_handler(commands=['turnon_notifications'])
        def turnon_notif_event(message):
            telegram_id = message.from_user.id
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            user_data = self.cursor.fetchone()
            if user_data:
                self.cursor.execute("UPDATE users SET notif_status = ? WHERE telegram_id = ?", ("1", telegram_id))
                self.conn.commit()
                self.bot.send_message(message.chat.id, "Your notifications are <b>turned on</b>!", parse_mode="HTML")

        # /turnoff_notifications command handler
        @self.bot.message_handler(commands=['turnoff_notifications'])
        def turnoff_notif_event(message):
            telegram_id = message.from_user.id
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            user_data = self.cursor.fetchone()
            if user_data:
                self.cursor.execute("UPDATE users SET notif_status = ? WHERE telegram_id = ?", ("0", telegram_id))
                self.conn.commit()
                self.bot.send_message(message.chat.id, "Your notifications are <b>turned off</b>!", parse_mode="HTML")

        # /pleasure command handler
        @self.bot.message_handler(commands=['pleasure'])
        def send_first_random_photo(message):
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            photo_number = random.randint(1, 20)
            photo_path = os.path.join(self.photo_folder, f'{photo_number}.jpg')
            markup = types.InlineKeyboardMarkup()
            button_more_pleasure = types.InlineKeyboardButton('More', callback_data='More_Pleasure')
            markup.add(button_more_pleasure)
            with open(photo_path, 'rb') as photo:
                self.bot.send_photo(message.chat.id, photo, reply_markup=markup)
            self.last_photo_number = photo_number

        # /update_database command handler
        @self.bot.message_handler(commands=['update_database'])
        def update_schedule_beginning(message):
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.bot.send_message(message.chat.id, "Please share your .ics file!")
            self.bot.register_next_step_handler(message, self.receive_ics_file)

        # Secret message for admin_status handler
        @self.bot.message_handler(func=lambda message: message.text == "1337")
        def handle_admin_status(message):
            telegram_id = message.from_user.id
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            user_data = self.cursor.fetchone()
            if user_data:
                self.cursor.execute("UPDATE users SET admin_status = ? WHERE telegram_id = ?", ("1", telegram_id))
                self.conn.commit()

########################################

    # Initialization of Main Menu
    def menu_init(self, message):
        telegram_id = message.from_user.id
        new_cursor = self.conn.cursor()
        new_cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        user_data = new_cursor.fetchone()
        new_cursor.close()
        markup = types.InlineKeyboardMarkup()
        button_schedule = types.InlineKeyboardButton('Schedule', callback_data='Schedule')
        button_homework = types.InlineKeyboardButton('Homework', callback_data='Homework')
        button_homework_adm = types.InlineKeyboardButton('Homework (Admin)', callback_data='Homework_Admin')
        if user_data and user_data[4] == "1":
            markup.add(button_schedule, button_homework)
            markup.add(button_homework_adm)
        else:
            markup.add(button_schedule, button_homework)
        with open(self.menu_photo_path, 'rb') as photo:
            self.bot.send_photo(chat_id=message.chat.id, photo=photo, reply_markup=markup)

    # Buttons callback hadling logic
    def glob_callback_handler(self):
        @self.bot.callback_query_handler(func=lambda call: True)
        def handle_change_choice(call):
            if call.data in ['1', '2']:
                self.process_choice(call)
            elif call.data == 'More_Pleasure':
                self.random_photo_update(call)
            elif call.data == 'Schedule':
                self.handle_schedule(call)
            elif call.data == 'Homework':
                self.handle_homework(call)
            elif call.data == 'Homework_Admin':
                self.handle_admin_homework(call)
            elif call.data == 'Menu':
                self.menu_buttons_and_other(call)
            elif call.data in ['1adj', '2adj']:
                self.admin_homework_proceed(call)
            elif call.data in ['1_1', '1_2', '1_3', '1_4', '1_5', '1_6', '2_1', '2_2', '2_3', '2_4', '2_5', '2_6']:
                self.write_adjust_homework(call)

    # Reinitializing menu but using call instead of message
    def menu_buttons_and_other(self, call):
        telegram_id = call.from_user.id
        self.cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        user_data = self.cursor.fetchone()
        markup = types.InlineKeyboardMarkup()
        button_schedule = types.InlineKeyboardButton('Schedule', callback_data='Schedule')
        button_homework = types.InlineKeyboardButton('Homework', callback_data='Homework')
        button_homework_adm = types.InlineKeyboardButton('Homework (Admin)', callback_data='Homework_Admin')
        if user_data and user_data[4] == "1":
            markup.add(button_schedule, button_homework)
            markup.add(button_homework_adm)
        else:
            markup.add(button_schedule, button_homework)
        with open(self.menu_photo_path, 'rb') as photo:
            media = types.InputMediaPhoto(photo, caption='')
            self.bot.edit_message_media(media=media, chat_id=call.message.chat.id, message_id=call.message.message_id)
        self.bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption='', reply_markup=markup)

    # Logic of recieving the filtering the file for database updating
    def receive_ics_file(self, message):
        try:
            if message.document:
                file_name = message.document.file_name
                if file_name in ["1Studenta_grafiks_24_25-P.ics", "2Studenta_grafiks_24_25-P.ics"]:
                    file_path = os.path.join(self.ics_source_file_dir, file_name)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    file_info = self.bot.get_file(message.document.file_id)
                    downloaded_file = self.bot.download_file(file_info.file_path)
                    file_path = os.path.join(self.ics_source_file_dir, file_name)
                    with open(file_path, 'wb') as new_file:
                        new_file.write(downloaded_file)

                    # Passing the file path to the method, that will process it and sending affirmation to the user
                    self.schedule_import_from_ics(file_path)
                    self.unique_latvian_names()
                    group_number = file_name[0]
                    self.bot.send_message(message.chat.id, f"The schedule for Your group #{group_number} is updated, Master!")
                else:
                    self.bot.send_message(message.chat.id, "Unfortunately the file is incorrect. Please send the file with a valid name...")
            else:
                self.bot.send_message(message.chat.id, 'Please try again! Consider sending an .ics file, that can be installed in ORTUS by For Students > Schedule > "<u>..here</u>" button. ', parse_mode="HTML")
        except Exception: logging.exception("RECEIVING ICS FILE: ")

    # Additional logic of random photo
    def random_photo_update(self, call):
        photo_number = random.randint(1, 20)
        while photo_number == self.last_photo_number:
            photo_number = random.randint(1,20)
        photo_path = os.path.join(self.photo_folder, f'{photo_number}.jpg')
        self.last_photo_number = photo_number
        markup = types.InlineKeyboardMarkup()
        button_more_pleasure = types.InlineKeyboardButton('More', callback_data='More_Pleasure')
        markup.add(button_more_pleasure)
        with open(photo_path, 'rb') as photo:
            media = types.InputMediaPhoto(photo)
            self.bot.edit_message_media(media=media, chat_id=call.message.chat.id, message_id=call.message.message_id)
        self.bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption="", reply_markup=markup)

    # Ask for group number
    def ask_for_choice(self, message_or_call):
        markup = types.InlineKeyboardMarkup()
        button1 = types.InlineKeyboardButton('1', callback_data='1')
        button2 = types.InlineKeyboardButton('2', callback_data='2')
        markup.add(button1, button2)
        if hasattr(message_or_call, 'message'):
            self.bot.edit_message_text(chat_id=message_or_call.message.chat.id, message_id=message_or_call.message.message_id, text="Choose your group number, Master...", reply_markup=markup)
        else:
            self.bot.send_message(message_or_call.chat.id, "Choose your group number, Master...", reply_markup=markup)

    # Creating or updating the profile of the user (Group number)
    def process_choice(self, call):
        telegram_id = call.from_user.id
        user_choice = call.data
        user_name = call.from_user.username
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        user_data = self.cursor.fetchone()
        if user_choice in ['1', '2']:
            if user_data: # Update existing user choice
                self.cursor.execute("UPDATE users SET choice = ?, username = ?, timestamp = ?, admin_status = ?, notif_status = ? WHERE telegram_id = ?",
                                (user_choice, user_name, timestamp, "0", "1", telegram_id ))
                self.conn.commit()
                self.bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                    text=f"Your group number is updated to: #{user_choice}", reply_markup=None)
            else: # Insert new user with their choice
                self.cursor.execute("INSERT INTO users (telegram_id, choice, username, timestamp, admin_status, notif_status) VALUES (?, ?, ?, ?, ?, ?)",
                                (telegram_id, user_choice, user_name, timestamp, "0", "1"))
                self.conn.commit()
                self.bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                    text=f"You have chosen group number: #{user_choice}", reply_markup=None)
        else:
            self.ask_for_choice(call)

########################################

    # If Schedule is chosen:
    def handle_schedule(self, call):
        telegram_id = call.from_user.id
        self.cursor.execute("SELECT choice FROM users WHERE telegram_id = ?", (telegram_id,))
        user_data = self.cursor.fetchone()
        if user_data and user_data[0] in [1, 2]:
            schedule_message = self.get_schedule(user_data, call)
            if user_data[0] in [1]: schedule_photo = self.schedule1_photo_path
            elif user_data[0] in [2]: schedule_photo = self.schedule2_photo_path
            with open(schedule_photo, 'rb') as photo:
                media = types.InputMediaPhoto(photo, caption=schedule_message, parse_mode="HTML")
                self.bot.edit_message_media(media=media, chat_id=call.message.chat.id, message_id=call.message.message_id)
            markup = types.InlineKeyboardMarkup()
            button_menu = types.InlineKeyboardButton("Menu", callback_data="Menu")
            markup.add(button_menu)
            self.bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=schedule_message, parse_mode="HTML", reply_markup=markup)
        else:
            self.ask_for_choice(call.message)

    # Forming and preparing the schedule text
    def get_schedule(self, user_data, call):
        schedule_data = None
        start_date = datetime.now()
        end_date = (start_date + timedelta(days=6)).strftime('%Y-%m-%d')
        start_date = start_date.strftime('%Y-%m-%d')
        table_name = f'schedule{user_data[0]}'
        try:
            self.cursor.execute(f"SELECT * FROM {table_name} WHERE start_date BETWEEN '{start_date}' AND '{end_date}'")
            schedule_data = self.cursor.fetchall()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e): self.bot.send_message(call.message.chat.id, "The table seems missing... please update the schedule by /update_database, Master!")
            else: logging.exception(f"SCHEDULE NOTIFICATION DB ACCESS: {e}")
        schedule_message = ''
        if schedule_data:
            tempdate = ''
            for row in schedule_data:
                date_str = row[0]
                year_num, month_num, day_sch = date_str.split('-')
                day_sch = int(day_sch)
                month_num = int(month_num)
                year_num = int(year_num)
                months_base = {1: "Jan",  2: "Feb",  3: "Mar",
                               4: "Apr",  5: "May",  6: "June",
                               7: "July", 8: "Aug",  9: "Sept",
                               10: "Oct", 11: "Nov", 12: "Dec"}
                eng_lectures = {1: "Lab. Mathematics", 2: "Lab. Data Structures", 3: "Lect. Software Automation", 4: "Lect. Mathematics", 5: "Lect.|Lab. Operations Research", 
                                6: "Lect.|Pr. Product Development", 7: "Pr. Mathematics", 8: "Lect. Data Structures", 9: "Lect.|Lab. Database Management"}
                input_date = datetime(year_num, month_num, day_sch)
                day_of_week = input_date.strftime("%a")
                month_sch = months_base.get(month_num)
                todays_date_for_sch = f"{day_sch} {month_sch}"
                self.cursor.execute("SELECT id, lecture FROM 'unique_latvian_lectures'")
                lecture_map = {row[1]: row[0] for row in self.cursor.fetchall()}
                lesson_id = lecture_map.get(row[3], None)
                english_lesson_name = eng_lectures.get(lesson_id)
                if row[0] == tempdate:
                    schedule_message += f"• <i>{english_lesson_name}\n({row[1]} - {row[2]}) ({row[4]})</i>\n\n" # Following lessons
                else:
                    schedule_message += f"\n⚜️  <u><b>{day_of_week} ({todays_date_for_sch}):</b></u>\n\n• <i>{english_lesson_name}\n({row[1]} - {row[2]}) ({row[4]})</i>\n\n" # First lesson of the day
                    tempdate = row[0]
            return schedule_message if schedule_message else "Unfortunately there is either error with database or no lessons found..."

########################################

    # If Homework is chosen:
    def handle_homework(self, call):
        telegram_id = call.from_user.id
        self.cursor.execute("SELECT choice FROM users WHERE telegram_id = ?", (telegram_id,))
        user_data = self.cursor.fetchone()
        if user_data and user_data[0] in [1, 2]:
            user_data = user_data[0]
            homework_message = self.homework_text_former(user_data)
            if user_data == 1: homework_photo = self.homework1_photo_path
            elif user_data == 2: homework_photo = self.homework2_photo_path
            with open(homework_photo, 'rb') as photo:
                media = types.InputMediaPhoto(photo, caption=homework_message, parse_mode="HTML")
                self.bot.edit_message_media(media=media, chat_id=call.message.chat.id, message_id=call.message.message_id)
            markup = types.InlineKeyboardMarkup()
            button_menu = types.InlineKeyboardButton("Menu", callback_data="Menu")
            markup.add(button_menu)
            self.bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=homework_message, parse_mode="HTML", reply_markup=markup)
        else:
            self.ask_for_choice(call.message)

    # If Admins Homework is chosen:
    def handle_admin_homework(self, call):
        subjects_titles_for_hw = {1: 'Data Structures', 2: 'Database management', 3: 'Mathematics', 4: 'Operations Research', 5: 'Product Development', 6: 'Software Automation'}
        for table_name in ['hw1', 'hw2']:
            self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, subject TEXT UNIQUE, hw_text TEXT, deadline TEXT)''')
            self.conn.commit()
            for subject in subjects_titles_for_hw.values():
                self.cursor.execute(f"INSERT OR IGNORE INTO {table_name} (subject) VALUES (?)", (subject,))
            self.conn.commit()

        # Initializing admin menu
        with open(self.admin_homework_photo_path, 'rb') as photo:
            media = types.InputMediaPhoto(photo, caption='Select the group you want to adjust:')
            self.bot.edit_message_media(media=media, chat_id=call.message.chat.id, message_id=call.message.message_id)
        markup = types.InlineKeyboardMarkup()
        button_group_adjust1 = types.InlineKeyboardButton("1", callback_data="1adj")
        button_group_adjust2 = types.InlineKeyboardButton("2", callback_data="2adj")
        markup.add(button_group_adjust1, button_group_adjust2)
        self.bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                      caption="Please, select the group you want to adjust, Master:", reply_markup=markup)

    # Proceeding with admin homework, letting to choose which specific subject admin wants to change
    def admin_homework_proceed(self, call):
        user_data = call.data[0]
        homework_message = self.homework_text_former(user_data)
        markup = types.InlineKeyboardMarkup()
        button_1hw = types.InlineKeyboardButton("1", callback_data=f"{user_data}_1")
        button_2hw = types.InlineKeyboardButton("2", callback_data=f"{user_data}_2")
        button_3hw = types.InlineKeyboardButton("3", callback_data=f"{user_data}_3")
        button_4hw = types.InlineKeyboardButton("4", callback_data=f"{user_data}_4")
        button_5hw = types.InlineKeyboardButton("5", callback_data=f"{user_data}_5")
        button_6hw = types.InlineKeyboardButton("6", callback_data=f"{user_data}_6")
        button_menu = types.InlineKeyboardButton("Menu", callback_data="Menu")
        markup.add(button_1hw, button_2hw, button_3hw)
        markup.add(button_4hw, button_5hw, button_6hw)
        markup.add(button_menu)
        self.bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=homework_message, parse_mode="HTML", reply_markup=markup)

    # Beginning of the editing process
    def write_adjust_homework(self, call):
        user_data, subject_to_change = call.data.split('_')
        message = self.bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                caption='⚜️  Please, type the <b>text</b> of the homework and its <b>deadline</b>, <i>(e.g: "Read presentations 20250530"; where 2025.. - year, ..05.. - month, ..30 - date)</i>, or:\n\n- Type <b>"Del"</b> to delete homework for this subject.\n- Type <b>"Back"</b> to go back without changes.',
                                parse_mode='HTML')
        self.bot.register_next_step_handler(message, lambda msg: self.process_homework_rewrite(msg, call, user_data, subject_to_change))

    # Performing changes in the database
    def process_homework_rewrite(self, message, call, user_data, subject_to_change):
        user_input = message.text.strip()
        if user_input.lower() == "del":
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            table_name = f'hw{user_data}'
            self.cursor.execute(f"UPDATE {table_name} SET hw_text = '', deadline = '' WHERE id = ?",
                                (subject_to_change,))
            self.conn.commit()
            self.admin_homework_proceed(call)
        elif user_input.lower() == "back":
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.admin_homework_proceed(call)
        elif re.match(r"^[\w\s]+ \d{8}$", user_input):
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            table_name = f'hw{user_data}'
            homework_text = user_input[:-8].strip()
            deadline = user_input[-8:]
            self.cursor.execute(f"UPDATE {table_name} SET hw_text = ?, deadline = ? WHERE id = ?",
                                (homework_text, deadline, subject_to_change))
            self.conn.commit()
            self.admin_homework_proceed(call)
        else:
            self.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            self.handle_admin_homework(call)

    # Method that forms the text of the homework (getting the group number as input, returning respective group's homework)
    def homework_text_former(self, user_data):
        table_name_hw = f'hw{user_data}'
        self.cursor.execute(f"SELECT id, subject, hw_text, deadline FROM {table_name_hw}")
        rows = self.cursor.fetchall()
        homework_message = ''
        for row in rows:
            if row[3] is not None and row[3] != '':
                deadline_date = datetime.strptime(row[3], "%Y%m%d").strftime("%d.%m")
                deadline = f'(dl {deadline_date})'
            else: deadline = ''
            if row[2] is not None and row[2] != '':
                hw_text = f'            •  {row[2]}'
            else: hw_text = ''
            homework_message += f"\n⚜️  <b>{row[0]}. <u>{row[1]}</u>:</b> \n<i>{hw_text} {deadline}</i>\n\n"
        return homework_message

########################################

    # Logic of updating the schedule from the sent file
    def schedule_import_from_ics(self, ics_file):
        previous_row = None
        file_name = os.path.basename(ics_file)
        table_number = file_name[0]
        table_name = f"schedule{table_number}"
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if self.cursor.fetchone():
            self.cursor.execute(f"DELETE FROM {table_name}")

        # Creating the table for schedule if it does not exist
        self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (
                            start_date TEXT,
                            start_time TEXT,
                            end_time TEXT,
                            title TEXT,
                            location TEXT,
                            end_date TEXT)''')
        with open(ics_file, 'rb') as sch:
            calendar = Calendar.from_ical(sch.read())

        # Checking if there is any end_date column and add if missing
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in self.cursor.fetchall()]
        if 'end_date' not in columns:
            self.cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN end_date TEXT")

        # Extracting the information from file
        for event in calendar.walk('VEVENT'):
            start = event.get('DTSTART').dt
            end = event.get('DTEND').dt
            title = event.get('SUMMARY')
            location = event.get('LOCATION')
            if location:
                location = location.replace("Rīga\\,", "").replace("Rīga,", "").strip()
                location = re.sub(r".*?(?=Zoom)", "", location).strip()
            start_date = start.strftime('%Y-%m-%d')
            start_time = start.strftime('%H:%M')
            end_date = end.strftime('%Y-%m-%d')
            end_time = end.strftime('%H:%M')
            current_row = (start_date, start_time, end_time, title, location, end_date)

            # Saving the data in the SQL base, deleting of end_date column and saving the table
            if current_row != previous_row:
                self.cursor.execute(f'''INSERT INTO {table_name} (start_date, start_time, end_time, title, location, end_date)
                            VALUES (?, ?, ?, ?, ?, ?)''', (start_date, start_time, end_time, title, location, end_date))
            previous_row = current_row
        self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}_temp AS SELECT start_date, start_time, end_time, title, location FROM {table_name}''')
        self.cursor.execute(f'DROP TABLE {table_name}')
        self.cursor.execute(f'ALTER TABLE {table_name}_temp RENAME TO {table_name}')
        self.conn.commit()

    # Extracting the unique names of the lectures from the schedule
    def unique_latvian_names(self):
        cursor = self.conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS "unique_latvian_lectures" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lecture TEXT UNIQUE)""")
        self.conn.commit()
        unique_titles = set()
        cursor.execute("SELECT title FROM schedule1")
        for row in cursor.fetchall():
            unique_titles.add(row[0])
        cursor.execute("SELECT title FROM schedule2")
        for row in cursor.fetchall():
            unique_titles.add(row[0])
        for title in unique_titles:
            cursor.execute("""INSERT OR IGNORE INTO "unique_latvian_lectures" (lecture) VALUES (?)""", (title,))
        self.conn.commit()
        cursor.close()

########################################

class Notif:
    def __init__(self, bot, conn):
    # Declaring several variables and paths in self, making connections with telegram API and DB
        self.bot = bot
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.scheduler = BackgroundScheduler(timezone=ZoneInfo("Europe/Riga"))
        self.current_directory = os.path.dirname(os.path.abspath(__file__))
        self.photo_folder = os.path.join(self.current_directory, '2b')
        self.notification_sch = os.path.join(self.photo_folder, 'notification_schedule.png')
        self.notification_hw = os.path.join(self.photo_folder, 'notification_homework.png')

        # Scheduler responsible for triggering other events at specific time
        self.scheduler.add_job(self.notification_schedule, 'cron', day_of_week='mon-fri', hour=8, minute=0)
        self.scheduler.add_job(self.notification_homework, 'cron', day_of_week='mon-fri', hour=20, minute=0)
        self.scheduler.start()

    # Sending morning notification with schedule for today
    def notification_schedule(self):
        group1_ids, group2_ids = self.get_tg_ids()
        if group1_ids:
            caption_sch = self.schedule_notif_text("1")
            for user_id in group1_ids:
                with open(self.notification_sch, 'rb') as photo:
                    self.bot.send_photo(user_id, photo, caption=caption_sch, parse_mode="HTML")
        if group2_ids:
            caption_sch = self.schedule_notif_text("2")
            for user_id in group2_ids:
                with open(self.notification_sch, 'rb') as photo:
                    self.bot.send_photo(user_id, photo, caption=caption_sch, parse_mode="HTML")

    # Sending evening notification with pending homework
    def notification_homework(self):
        group1_ids, group2_ids = self.get_tg_ids()
        if group1_ids:
            caption_hw = self.homework_notif_text("1")
            for user_id in group1_ids:
                with open(self.notification_hw, 'rb') as photo:
                    self.bot.send_photo(user_id, photo, caption=caption_hw, parse_mode="HTML")
        if group2_ids:
            caption_hw = self.homework_notif_text("2")
            for user_id in group2_ids:
                with open(self.notification_hw, 'rb') as photo:
                    self.bot.send_photo(user_id, photo, caption=caption_hw, parse_mode="HTML")

    # This method gets the telegram ids of the users from both groups for mailing
    def get_tg_ids(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT telegram_id FROM users WHERE choice = 1 AND notif_status != 0")
        choice_1_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT telegram_id FROM users WHERE choice = 2 AND notif_status != 0")
        choice_2_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return choice_1_ids, choice_2_ids

    # Method for forming and preparing the text of morning notification with the schedule for today
    def schedule_notif_text(self, group):
        cursor = self.conn.cursor()
        table_sch = f'schedule{group}'
        today_date = datetime.now().strftime('%Y-%m-%d')
        try:
            cursor.execute(f"SELECT * FROM {table_sch} WHERE start_date = ?", (today_date,))
            schedule_data = cursor.fetchall()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e): return "The table seems missing... please update the schedule by /update_database, Master!"
            else: logging.exception(f"SCHEDULE NOTIFICATION DB ACCESS: {e}")
        if schedule_data:
            months_base = {1: "Jan",  2: "Feb",  3: "Mar",
                            4: "Apr",  5: "May",  6: "June",
                            7: "July", 8: "Aug",  9: "Sept",
                            10: "Oct", 11: "Nov", 12: "Dec"}
            eng_lectures = {1: "Lab. Mathematics", 2: "Lab. Data Structures", 3: "Lect. Software Automation", 4: "Lect. Mathematics", 5: "Lect.|Lab. Operations Research", 
                            6: "Lect.|Pr. Product Development", 7: "Pr. Mathematics", 8: "Lect. Data Structures", 9: "Lect.|Lab. Database Management"}
            schedule_message_notif = ""
            schedule_body_message = ""
            date_full = None
            cursor.execute("SELECT id, lecture FROM 'unique_latvian_lectures'")
            lecture_map = {row[1]: row[0] for row in cursor.fetchall()}
            for row in schedule_data:
                date_str = row[0]
                year_int, month_int, day_int = date_str.split('-')
                day_int = int(day_int)
                month_int = int(month_int)
                year_int = int(year_int)
                month_sch = months_base.get(month_int)
                custom_date = datetime(year_int, month_int, day_int)
                day_of_week = custom_date.strftime("%a")
                todays_date_for_sch = f"{day_int} {month_sch}"
                lesson_id = lecture_map.get(row[3], None)
                english_lesson_name = eng_lectures.get(lesson_id, "Unknown Lesson")
                date_full = f'{day_of_week}/{todays_date_for_sch}'
                schedule_body_message += f"\n• <i>{english_lesson_name}\n({row[1]} - {row[2]}) ({row[4]})</i>\n"
            schedule_message_notif += f'<b>Good Morning, Master...\nThis is your schedule for (<u>{date_full}</u>):\n<i>Please consider attending the lessons on time!</i></b>\n\n⚜️\n'
            schedule_message_notif += schedule_body_message
        else:
            schedule_message_notif = "No lessons scheduled for today."
        return schedule_message_notif

    # Method for forming and preparing the text of evening notification with the pending homework
    def homework_notif_text(self, group):
        try:
            table_hw = f'hw{group}'
            cursor = self.conn.cursor()
            today_str = datetime.now().strftime("%Y%m%d")
            cursor.execute(f"""UPDATE {table_hw} SET hw_text = '', deadline = '' WHERE deadline <= ?""", (today_str,))
            self.conn.commit()
            cursor.execute(f"SELECT subject, hw_text, deadline FROM {table_hw} WHERE hw_text IS NOT NULL AND hw_text != '' AND deadline IS NOT NULL AND deadline != '' ORDER BY deadline")
            homeworks = cursor.fetchall()
            cursor.close()
            if homeworks:
                today = datetime.now()
                next_day = today + timedelta(days=1)
                today_str = today.strftime("%Y%m%d")
                next_day_str = next_day.strftime("%Y%m%d")
                homework_text = '<b>Good Evening, Master...\nThis is the list of Your pending hometasks:\n<i>Please consider completing those on time!</i></b>\n\n\n'
                urgent_tasks = []
                regular_tasks = []
                for subject, hw_text, deadline in homeworks:
                    correct_dl = f'{deadline[6:8]}.{deadline[4:6]}'
                    if deadline == today_str or deadline == next_day_str: urgent_tasks.append(f'⚜️  <b><u>{subject}</u>:\n        •  <i>{hw_text} (dl {correct_dl})</i></b>\n\n')
                    else: regular_tasks.append(f'⚜️  <u>{subject}</u>:\n        •  <i>{hw_text} (dl {correct_dl})</i>\n\n')
                if urgent_tasks: homework_text += "\n".join(urgent_tasks) + "<b>________________________________________</b>\n\n\n"
                if regular_tasks: homework_text += "\n".join(regular_tasks)
                return homework_text
            else:
                return "<b>Good Evening, Master...\nLooks like currently you have no hometasks!\n<i>Please consider checking the homework manually!</i></b>"
        except Exception: logging.exception("HW TEXT NOTIFICATION FORMING: ")
        return "Error while retrieving info and forming, please check logs. "

########################################

if __name__ == "__main__":
    notif_instance = None
    try:
        thread_one = threading.Thread(target=Main, args=(bot, conn))
        thread_two = threading.Thread(target=Notif, args=(bot, conn))
        thread_one.start()
        thread_two.start()
        thread_one.join()
        thread_two.join()
    except Exception: logging.exception("THREADING PROBLEM: ")
    finally:
        if notif_instance: notif_instance.shutdown()
        conn.close()