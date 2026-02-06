import base64
import os

# Читаем base64 из переменной окружения
session_base64 = os.getenv('SESSION_BASE64')

if session_base64:
    # Декодируем и сохраняем
    session_data = base64.b64decode(session_base64)
    with open('bot_session.session', 'wb') as f:
        f.write(session_data)
    print("✅ Session decoded successfully")
else:
    print("❌ SESSION_BASE64 not found")
