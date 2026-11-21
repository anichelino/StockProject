import os
import sys

import requests


def get_supabase():
    """Crea il client Supabase leggendo i secrets (con controlli chiari)."""
    from supabase import create_client

    url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = (os.getenv("SUPABASE_KEY") or "").strip()
    if not url.startswith("https://"):
        sys.exit(
            "SUPABASE_URL mancante o non valido. Deve essere del tipo "
            "https://xxxxxxxx.supabase.co. Controlla il secret SUPABASE_URL su GitHub."
        )
    if not key:
        sys.exit("SUPABASE_KEY mancante. Controlla il secret SUPABASE_KEY su GitHub.")
    return create_client(url, key)


def send_telegram(text):
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not (token and chat_id):
        print("Token o chat ID Telegram non impostati: messaggio non inviato.")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=15)
    if r.status_code != 200:
        print(f"Errore invio Telegram: {r.text}")
