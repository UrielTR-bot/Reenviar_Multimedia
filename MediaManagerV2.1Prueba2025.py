import logging
import time
import threading
import re
import sqlite3
import os
from collections import defaultdict

from pyrogram import Client, filters
from pyrogram.types import (
    Message, InputMediaPhoto, InputMediaVideo,
    InputMediaDocument, InputMediaAudio
)
from pyrogram.errors import RPCError

# ----------------------------
# Configuración del bot
# ----------------------------
api_id = 28651675  # Tu API ID
api_hash = "6438bee32a12da56706170b8f34fb487"  # Tu API HASH
bot_token = "7024044929:AAFy-R7CCDfHuRI9Y6ZGm6rA9tP7lgeaFvI"  # Tu token del bot

CHANNEL_ID = -1002169066047      # ID numérico de tu canal
DISCUSSION_ID = -1002190873277   # ID numérico del grupo de discusión vinculado

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Client("bot_discussion", api_id=api_id, api_hash=api_hash, bot_token=bot_token)

# ----------------------------
# Base de datos SQLite
# ----------------------------
DB_FILE = "etiquetas.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS etiquetas (
            etiqueta TEXT PRIMARY KEY,
            group_msg_id INTEGER
        );
    """)
    conn.commit()
    conn.close()

def get_group_msg_id(etiqueta: str) -> int:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT group_msg_id FROM etiquetas WHERE etiqueta = ?", (etiqueta,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0

def set_group_msg_id(etiqueta: str, group_msg_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO etiquetas (etiqueta, group_msg_id)
        VALUES (?, ?)
        ON CONFLICT(etiqueta)
        DO UPDATE SET group_msg_id=excluded.group_msg_id
    """, (etiqueta, group_msg_id))
    conn.commit()
    conn.close()

pending_mirrors = {}
albums_in_progress = defaultdict(lambda: {
    "messages": [],
    "timer": None,
    "etiquetas": set()
})
ALBUM_TIMEOUT = 10

# ----------------------------
# Funciones de etiquetas
# ----------------------------
def extraer_etiquetas(texto: str) -> set:
    # Captura # seguido de cualquier secuencia sin espacios (incluye puntos)
    return set(re.findall(r"#\S+", texto or ""))

def limpiar_otras_etiquetas(caption_original: str, etiqueta_principal: str) -> str:
    todas = extraer_etiquetas(caption_original)
    texto_limpio = caption_original
    for t in todas:
        if t != etiqueta_principal:
            texto_limpio = texto_limpio.replace(t, "")
    return texto_limpio.strip()

# ----------------------------
# COMANDOS: ver / borrar etiquetas
# ----------------------------
@bot.on_message(filters.command("ver_etiquetas") & filters.private)
def ver_etiquetas(client, message):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT etiqueta, group_msg_id FROM etiquetas")
    rows = c.fetchall()
    conn.close()
    if not rows:
        return message.reply("No hay etiquetas guardadas.")
    texto = "Etiquetas guardadas:\n\n" + "\n".join(f"- {et} ⇒ {gid}" for et, gid in rows)
    message.reply(texto)

@bot.on_message(filters.command("borrar_etiqueta") & filters.private)
def borrar_etiqueta(client, message):
    parts = message.text.split()
    if len(parts) < 2:
        return message.reply("Uso: /borrar_etiqueta #etiqueta")
    et = parts[1]
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM etiquetas WHERE etiqueta = ?", (et,))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    message.reply(f"Etiqueta {et} {'eliminada' if deleted else 'no encontrada'}.")

@bot.on_message(filters.command("borrar_todo") & filters.private)
def borrar_todo(client, message):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM etiquetas")
    count = c.rowcount
    conn.commit()
    conn.close()
    message.reply(f"Se borraron {count} etiquetas.")

# ----------------------------
# 1) Capturar mensaje espejo en grupo
# ----------------------------
@bot.on_message(filters.chat(DISCUSSION_ID))
def on_group_message(client: Client, message: Message):
    if not hasattr(message, "forward_from_message_id"):
        return
    fwd_id = message.forward_from_message_id
    if fwd_id in pending_mirrors:
        etiqueta = pending_mirrors[fwd_id]
        set_group_msg_id(etiqueta, message.id)
        logger.info(f"Espejo capturado: {etiqueta} ⇒ {message.id}")
        del pending_mirrors[fwd_id]

# ----------------------------
# 3) Crear post y registrar espejo pendiente
# ----------------------------
def asegurar_etiqueta_y_espello(etiqueta: str) -> int:
    existing_id = get_group_msg_id(etiqueta)
    if existing_id:
        return existing_id
    post = bot.send_message(CHANNEL_ID, etiqueta)
    pending_mirrors[post.id] = etiqueta
    start = time.time()
    while time.time() - start < 10:
        gid = get_group_msg_id(etiqueta)
        if gid:
            return gid
        time.sleep(1)
    logger.error(f"No llegó espejo para {etiqueta}")
    return 0

# ----------------------------
# 2) Manejo de álbumes
# ----------------------------
def finalize_album(media_group_id):
    data = albums_in_progress[media_group_id]
    msgs = data["messages"]
    tags = data["etiquetas"]
    msgs.sort(key=lambda x: x.date)
    if not tags:
        del albums_in_progress[media_group_id]
        return
    for tag in tags:
        gid = asegurar_etiqueta_y_espello(tag)
        if not gid:
            continue
        media = []; first=True
        for m in msgs:
            cap = limpiar_otras_etiquetas(m.caption or "", tag) if first else ""
            first=False
            if m.photo:    media.append(InputMediaPhoto(m.photo.file_id, caption=cap))
            elif m.video:  media.append(InputMediaVideo(m.video.file_id, caption=cap))
            elif m.document: media.append(InputMediaDocument(m.document.file_id, caption=cap))
            elif m.audio:  media.append(InputMediaAudio(m.audio.file_id, caption=cap))
        if media:
            try:
                bot.send_media_group(DISCUSSION_ID, media=media, reply_to_message_id=gid)
            except RPCError as e:
                logger.error(f"Error álbum{media_group_id}->{tag}: {e}")
    del albums_in_progress[media_group_id]

def reset_album_timer(mgid):
    if albums_in_progress[mgid]["timer"]:
        albums_in_progress[mgid]["timer"].cancel()
    t=threading.Timer(ALBUM_TIMEOUT, finalize_album, [mgid])
    albums_in_progress[mgid]["timer"]=t; t.start()

# ----------------------------
# 4) Handler genérico
# ----------------------------
@bot.on_message(filters.private)
def on_private_message(client: Client, message: Message):
    if message.media_group_id:
        mg=message.media_group_id
        albums_in_progress[mg]["messages"].append(message)
        albums_in_progress[mg]["etiquetas"].update(extraer_etiquetas(message.caption or message.text or ""))
        reset_album_timer(mg)
    else:
        txt=message.caption or message.text or ""
        tags=extraer_etiquetas(txt)
        if not tags:
            return message.reply("No encontré etiquetas.")
        for tag in tags:
            gid=asegurar_etiqueta_y_espello(tag)
            if not gid: continue
            cap=limpiar_otras_etiquetas(txt,tag)
            try:
                if message.photo:
                    bot.send_photo(DISCUSSION_ID, message.photo.file_id, caption=cap, reply_to_message_id=gid)
                elif message.video:
                    bot.send_video(DISCUSSION_ID, message.video.file_id, caption=cap, reply_to_message_id=gid)
                elif message.document:
                    bot.send_document(DISCUSSION_ID, message.document.file_id, caption=cap, reply_to_message_id=gid)
                elif message.audio:
                    bot.send_audio(DISCUSSION_ID, message.audio.file_id, caption=cap, reply_to_message_id=gid)
                else:
                    bot.send_message(DISCUSSION_ID, cap, reply_to_message_id=gid)
            except RPCError as e:
                logger.error(f"Error al reenviar msg {message.id}->{tag}: {e}")
        message.reply(f"Publicado en {len(tags)} etiqueta(s).")

# ----------------------------
# Iniciar el bot
# ----------------------------
if __name__ == "__main__":
    init_db()
    bot.run()
