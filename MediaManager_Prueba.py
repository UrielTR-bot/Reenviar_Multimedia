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
api_id = 28651675
api_hash = "6438bee32a12da56706170b8f34fb487"
bot_token = "7024044929:AAFy-R7CCDfHuRI9Y6ZGm6rA9tP7lgeaFvI"

CHANNEL_ID = -1002169066047
DISCUSSION_ID = -1002190873277

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Client("bot_discussion", api_id=api_id, api_hash=api_hash, bot_token=bot_token)

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

# ----------------------------
# Comandos de gestión de etiquetas
# ----------------------------
@bot.on_message(filters.command("ver_etiquetas") & filters.private)
def ver_etiquetas(client, message):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT etiqueta, group_msg_id FROM etiquetas")
    rows = c.fetchall()
    conn.close()

    if not rows:
        message.reply("No hay etiquetas guardadas.")
        return

    texto = "Etiquetas guardadas:\n\n"
    for (et, gid) in rows:
        texto += f"- {et} => group_msg_id={gid}\n"

    message.reply(texto)

@bot.on_message(filters.command("borrar_etiqueta") & filters.private)
def borrar_etiqueta(client, message):
    parts = message.text.split()
    if len(parts) < 2:
        message.reply("Uso: /borrar_etiqueta #etiqueta")
        return

    etiqueta_a_borrar = parts[1]
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM etiquetas WHERE etiqueta = ?", (etiqueta_a_borrar,))
    rows_deleted = c.rowcount
    conn.commit()
    conn.close()

    if rows_deleted > 0:
        message.reply(f"Etiqueta {etiqueta_a_borrar} eliminada.")
    else:
        message.reply(f"No se encontró la etiqueta {etiqueta_a_borrar}.")

@bot.on_message(filters.command("borrar_todo") & filters.private)
def borrar_todo(client, message):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM etiquetas")
    rows_deleted = c.rowcount
    conn.commit()
    conn.close()

    message.reply(f"Se han eliminado {rows_deleted} etiquetas en total.")

# ----------------------------
# Diccionarios temporales
# ----------------------------
pending_mirrors = {}
albums_in_progress = defaultdict(lambda: {
    "messages": [],
    "timer": None,
    "etiquetas": set()
})

ALBUM_TIMEOUT = 10

def extraer_etiquetas(texto: str) -> set:
    pattern = r"#\w+"
    return set(re.findall(pattern, texto or ""))

def limpiar_otras_etiquetas(caption_original: str, etiqueta_principal: str) -> str:
    todas = extraer_etiquetas(caption_original)
    texto_limpio = caption_original
    for t in todas:
        if t != etiqueta_principal:
            texto_limpio = texto_limpio.replace(t, "")
    return texto_limpio.strip()

@bot.on_message(filters.chat(DISCUSSION_ID))
def on_group_message(client: Client, message: Message):
    if not hasattr(message, "forward_from_message_id"):
        return

    fwd_id = message.forward_from_message_id
    if fwd_id in pending_mirrors:
        etiqueta = pending_mirrors[fwd_id]
        set_group_msg_id(etiqueta, message.id)
        logger.info(f"Mensaje espejo para etiqueta {etiqueta} capturado: group_msg_id={message.id}")
        del pending_mirrors[fwd_id]

def asegurar_etiqueta_y_espelho(etiqueta: str) -> int:
    existing_id = get_group_msg_id(etiqueta)
    if existing_id != 0:
        return existing_id

    post = bot.send_message(CHANNEL_ID, etiqueta)
    post_id = post.id
    logger.info(f"Post creado en canal para {etiqueta} => channel_msg_id={post_id}")

    pending_mirrors[post_id] = etiqueta

    start = time.time()
    while True:
        existing_id = get_group_msg_id(etiqueta)
        if existing_id != 0:
            return existing_id
        if time.time() - start > 10:
            break
        time.sleep(1)

    logger.error(f"No llegó el espejo para {etiqueta} tras 10s.")
    return 0

def finalize_album(media_group_id):
    data = albums_in_progress[media_group_id]
    msgs = data["messages"]
    all_tags = data["etiquetas"]

    msgs.sort(key=lambda x: x.date)

    if not all_tags:
        logger.info(f"Álbum {media_group_id} sin etiquetas. Se ignora.")
        del albums_in_progress[media_group_id]
        return

    logger.info(f"Finalizando álbum {media_group_id} con {len(msgs)} msgs y etiquetas={all_tags}")

    for tag in all_tags:
        group_msg_id = asegurar_etiqueta_y_espelho(tag)
        if not group_msg_id:
            logger.error(f"No se pudo obtener espejo para {tag}. Se omite.")
            continue

        media = []
        first = True
        for am in msgs:
            original_caption = am.caption or ""
            if first and original_caption:
                nueva_caption = limpiar_otras_etiquetas(original_caption, tag)
                first = False
            else:
                nueva_caption = ""

            if am.photo:
                media.append(InputMediaPhoto(am.photo.file_id, caption=nueva_caption))
            elif am.video:
                media.append(InputMediaVideo(am.video.file_id, caption=nueva_caption))
            elif am.document:
                media.append(InputMediaDocument(am.document.file_id, caption=nueva_caption))
            elif am.audio:
                media.append(InputMediaAudio(am.audio.file_id, caption=nueva_caption))

        if not media:
            continue

        try:
            bot.send_media_group(
                chat_id=DISCUSSION_ID,
                media=media,
                reply_to_message_id=group_msg_id
            )
            logger.info(f"Álbum {media_group_id} enviado a etiqueta {tag}.")
        except RPCError as e:
            logger.error(f"Error al enviar álbum {media_group_id} a {tag}: {e}")

    del albums_in_progress[media_group_id]

def reset_album_timer(mgroup_id):
    if albums_in_progress[mgroup_id]["timer"]:
        albums_in_progress[mgroup_id]["timer"].cancel()

    t = threading.Timer(ALBUM_TIMEOUT, finalize_album, [mgroup_id])
    albums_in_progress[mgroup_id]["timer"] = t
    t.start()

# Handler genérico al final
@bot.on_message(filters.private)
def on_private_message(client: Client, message: Message):
    if message.media_group_id:
        mgid = message.media_group_id
        albums_in_progress[mgid]["messages"].append(message)
        texto = (message.caption or message.text or "")
        found_tags = extraer_etiquetas(texto)
        albums_in_progress[mgid]["etiquetas"].update(found_tags)
        reset_album_timer(mgid)
    else:
        texto = (message.caption or message.text or "")
        found_tags = extraer_etiquetas(texto)
        if not found_tags:
            message.reply("No encontré ninguna etiqueta (#).")
            return

        logger.info(f"Mensaje suelto con etiquetas: {found_tags}")

        for tag in found_tags:
            group_msg_id = asegurar_etiqueta_y_espelho(tag)
            if not group_msg_id:
                message.reply(f"No se pudo obtener espejo para {tag}.")
                continue

            nueva_caption = limpiar_otras_etiquetas(texto, tag)

            if message.photo:
                try:
                    bot.send_photo(
                        chat_id=DISCUSSION_ID,
                        photo=message.photo.file_id,
                        caption=nueva_caption,
                        reply_to_message_id=group_msg_id
                    )
                except RPCError as e:
                    logger.error(f"Error al enviar foto a {tag}: {e}")
            elif message.video:
                try:
                    bot.send_video(
                        chat_id=DISCUSSION_ID,
                        video=message.video.file_id,
                        caption=nueva_caption,
                        reply_to_message_id=group_msg_id
                    )
                except RPCError as e:
                    logger.error(f"Error al enviar video a {tag}: {e}")
            elif message.document:
                try:
                    bot.send_document(
                        chat_id=DISCUSSION_ID,
                        document=message.document.file_id,
                        caption=nueva_caption,
                        reply_to_message_id=group_msg_id
                    )
                except RPCError as e:
                    logger.error(f"Error al enviar doc a {tag}: {e}")
            elif message.audio:
                try:
                    bot.send_audio(
                        chat_id=DISCUSSION_ID,
                        audio=message.audio.file_id,
                        caption=nueva_caption,
                        reply_to_message_id=group_msg_id
                    )
                except RPCError as e:
                    logger.error(f"Error al enviar audio a {tag}: {e}")
            else:
                bot.send_message(
                    chat_id=DISCUSSION_ID,
                    text=nueva_caption,
                    reply_to_message_id=group_msg_id
                )

        message.reply(f"¡Listo! Tu mensaje se publicó en {len(found_tags)} etiqueta(s), omitiendo las demás etiquetas.")

if __name__ == "__main__":
    init_db()
    bot.run()
