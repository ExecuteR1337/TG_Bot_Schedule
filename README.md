# 📅 Telegram Schedule Bot 

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![Telegram](https://img.shields.io/badge/Telegram-Bot-blue?logo=telegram)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

A lightweight Telegram bot for managing personal or group schedules with intuitive commands and reminders.

**Co-created by**: [ExecuteR1337](https://github.com/ExecuteR1337) & [AmirIst](https://github.com/AmirIst)

---

## ✨ Features
- View daily/weekly/monthly schedules
- Add/edit/delete events
- Set reminders
- Multi-user support
- Timezone-aware

## 🛠 Tech Stack
- **Python 3.9+**
- **aiogram** (Telegram Bot API)
- **SQLite** (Database)
- **Docker** (Deployment)

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Telegram Bot Token from [@BotFather](https://t.me/BotFather)

### Installation
```bash
git clone https://github.com/ExecuteR1337/TG_Bot_Schedule.git
cd TG_Bot_Schedule
pip install -r requirements.txt
```

### Configuration
Create .env file: 
```ini
BOT_TOKEN=your_bot_token_here
```

Run:
```bash
python main.py
```

Docker
```bash
docker build -t schedule_bot .
docker run -d --name bot_instance schedule_bot
```
