FROM python:3.10-slim

# نصب ابزارهای سیستمی مورد نیاز (مگا و اف‌ام‌پیگ)
RUN apt-get update && apt-get install -y \
    megatools \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# تعیین پوشه کاری در سرور
WORKDIR /app

# کپی کردن فایل نیازمندی‌ها و نصب پایتون پکیج‌ها
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# کپی کردن تمام فایل‌های پروژه
COPY . .

# دستور اجرای ربات
CMD ["python", "bot.py"]
