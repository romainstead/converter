import smtplib
from email.mime.text import MIMEText

# TODO: весь функционал отгрузки через SMTP

def send_message():
    # Настройки
    sender = "xxxx"
    recipient = "xxxx"
    password = "xxxx"  # Для Gmail лучше использовать App Password
    # Создаем сообщение
    msg = MIMEText("тест смтп.")
    msg['Subject'] = "Тестовое письмо"
    msg['From'] = sender
    msg['To'] = recipient

    # Подключение к серверу и отправка
    try:
        with smtplib.SMTP("letter.tpu.ru", 587) as server:
            server.starttls()  # Включаем шифрование
            server.login(sender, password)
            server.send_message(msg)
        print("Письмо успешно отправлено!")
    except Exception as e:
        print(f"Ошибка: {e}")

# if __name__ == '__main__':
#     send_message()