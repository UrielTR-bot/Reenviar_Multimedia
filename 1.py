import logging
import time
import threading
import re
import sqlite3
from collections import defaultdict
from string import ascii_uppercase

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InputMediaPhoto, InputMediaVideo,
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

bot = Client(
    "bot_discussion",
    api_id=api_id,
    api_hash=api_hash,
    bot_token=bot_token
)

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
    c.execute("""
        CREATE TABLE IF NOT EXISTS index_pages (
            initial TEXT PRIMARY KEY,
            message_id INTEGER
        );
    """)
    conn.commit()
    conn.close()

init_db()

# ----------------------------
# Helpers SQLite
# ----------------------------
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
        ON CONFLICT(etiqueta) DO UPDATE SET group_msg_id=excluded.group_msg_id
    """, (etiqueta, group_msg_id))
    conn.commit()
    conn.close()

def get_index_pages() -> dict:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT initial, message_id FROM index_pages")
    rows = dict(c.fetchall())
    conn.close()
    return rows

def set_index_page(initial: str, message_id: int):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO index_pages (initial, message_id)
        VALUES (?, ?)
        ON CONFLICT(initial) DO UPDATE SET message_id=excluded.message_id
    """, (initial, message_id))
    conn.commit()
    conn.close()

# ----------------------------
# Funciones de etiquetas
# ----------------------------
def extraer_etiquetas(texto: str) -> set:
    return set(re.findall(r"#\S+", texto or ""))

def limpiar_otras_etiquetas(caption: str, tag: str) -> str:
    todas = extraer_etiquetas(caption)
    text = caption
    for t in todas:
        if t != tag:
            text = text.replace(t, "")
    return text.strip()

# ----------------------------
# Sesiones de agrupamiento por usuario
# ----------------------------
group_sessions = defaultdict(list)
# user_id -> list of Message

# ----------------------------
# COMANDOS DE AGRUPAMIENTO
# ----------------------------
@bot.on_message(filters.command("cancelar") & filters.private)
def cmd_cancelar(client: Client, message: Message):
    uid = message.from_user.id
    if group_sessions.get(uid):
        group_sessions.pop(uid)
        message.reply("❌ Agrupamiento cancelado.")
    else:
        message.reply("No tienes una sesión activa.")

@bot.on_message(filters.command("etiquetar") & filters.private)
def cmd_etiquetar(client: Client, message: Message):
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].startswith("#"):
        return message.reply("Uso: /etiquetar #nombreDeEtiqueta")
    etiqueta = parts[1]
    uid = message.from_user.id
    msgs = group_sessions.get(uid)
    if not msgs:
        return message.reply("No tienes medios agrupados. Envía primero imágenes o videos sin etiqueta.")
    
    enviado = 0
    # Agrupar por álbum
    album_map = defaultdict(list)
    for msg in msgs:
        # Aseguramos key como cadena
        key = str(msg.media_group_id) if msg.media_group_id else f"single_{msg.id}"
        album_map[key].append(msg)
    
    for key, batch in album_map.items():
        gid = asegurar_etiqueta_y_espello(etiqueta)
        if not gid:
            continue
        
        if key.startswith("single_"):
            # Medio único
            m = batch[0]
            try:
                client.copy_message(
                    chat_id=DISCUSSION_ID,
                    from_chat_id=m.chat.id,
                    message_id=m.id,
                    reply_to_message_id=gid
                )
                enviado += 1
            except RPCError as e:
                logger.error(f"Error reenviando msg {m.id}: {e}")
        else:
            # Álbum completo
            media = []
            first = True
            for m in sorted(batch, key=lambda x: x.date):
                cap = m.caption if (first and m.caption) else ""
                first = False
                if m.photo:
                    media.append(InputMediaPhoto(m.photo.file_id, caption=cap))
                elif m.video:
                    media.append(InputMediaVideo(m.video.file_id, caption=cap))
                elif m.document:
                    media.append(InputMediaDocument(m.document.file_id, caption=cap))
                elif m.audio:
                    media.append(InputMediaAudio(m.audio.file_id, caption=cap))
            if media:
                try:
                    client.send_media_group(
                        chat_id=DISCUSSION_ID,
                        media=media,
                        reply_to_message_id=gid
                    )
                    enviado += len(media)
                except RPCError as e:
                    logger.error(f"Error reenviando álbum {key}: {e}")

    group_sessions.pop(uid, None)
    message.reply(f"✅ {enviado} medios etiquetados como {etiqueta}.")

# ----------------------------
# Handler para borrar aviso de fijado
# ----------------------------
@bot.on_message(filters.chat(CHANNEL_ID) & filters.service)
def eliminar_aviso_fijado(client: Client, message: Message):
    if message.pinned_message:
        try:
            client.delete_messages(CHANNEL_ID, message.id)
        except:
            pass

# ----------------------------
# COMANDOS: ver / borrar etiquetas
# ----------------------------
@bot.on_message(filters.command("ver_etiquetas") & filters.private)
def ver_etiquetas(client: Client, message: Message):
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
def borrar_etiqueta(client: Client, message: Message):
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
def borrar_todo(client: Client, message: Message):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM etiquetas")
    count = c.rowcount
    conn.commit()
    conn.close()
    message.reply(f"Se borraron {count} etiquetas.")

# ----------------------------
# COMANDO /indice
# ----------------------------
@bot.on_message(filters.command("indice") & filters.private)
def indice(client: Client, message: Message):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT etiqueta FROM etiquetas")
    all_tags = [r[0] for r in c.fetchall()]
    conn.close()
    if not all_tags:
        return message.reply("No hay etiquetas para indexar.")
    groups = defaultdict(list)
    for tag in all_tags:
        init = tag[1].upper()
        if init not in ascii_uppercase:
            init = "0-9"
        groups[init].append(tag)
    pages = get_index_pages()
    actuales = set(groups.keys())
    anteriores = set(pages.keys())
    # Eliminar obsoletos
    for letra in anteriores - actuales:
        try:
            client.delete_messages(CHANNEL_ID, pages[letra])
        except:
            pass
        conn = sqlite3.connect(DB_FILE)
        conn.execute("DELETE FROM index_pages WHERE initial = ?", (letra,))
        conn.commit()
        conn.close()
    # Crear/editar índices
    for initial, tags in groups.items():
        tags.sort(key=str.lower)
        text = f"📑 Etiquetas «{initial}»\n\n" + "\n".join(tags)
        if initial in pages:
            try:
                client.edit_message_text(CHANNEL_ID, pages[initial], text=text)
                continue
            except RPCError:
                client.delete_messages(CHANNEL_ID, pages[initial])
                pages.pop(initial, None)
        msg = client.send_message(CHANNEL_ID, text)
        client.pin_chat_message(CHANNEL_ID, msg.id, disable_notification=True)
        set_index_page(initial, msg.id)
        pages[initial] = msg.id
    message.reply("✅ Índice actualizado.")

# ----------------------------
# Resto: espejos, álbumes, reenvío
# ----------------------------
pending_mirrors = {}
albums_in_progress = defaultdict(lambda: {"messages": [], "timer": None, "etiquetas": set()})
ALBUM_TIMEOUT = 10

@bot.on_message(filters.chat(DISCUSSION_ID))
def on_group_message(client: Client, message: Message):
    if not hasattr(message, "forward_from_message_id"):
        return
    fwd = message.forward_from_message_id
    if fwd in pending_mirrors:
        tag = pending_mirrors[fwd]
        set_group_msg_id(tag, message.id)
        del pending_mirrors[fwd]

def asegurar_etiqueta_y_espello(etiqueta: str) -> int:
    gid = get_group_msg_id(etiqueta)
    if gid:
        return gid
    post = bot.send_message(CHANNEL_ID, etiqueta)
    pending_mirrors[post.id] = etiqueta
    start = time.time()
    while time.time() - start < 10:
        gid = get_group_msg_id(etiqueta)
        if gid:
            return gid
        time.sleep(1)
    return 0

def finalize_album(mgid):
    data = albums_in_progress[mgid]
    msgs, tags = data["messages"], data["etiquetas"]
    msgs.sort(key=lambda x: x.date)
    if not tags:
        del albums_in_progress[mgid]
        return
    for tag in tags:
        gid = asegurar_etiqueta_y_espello(tag)
        if not gid:
            continue
        media = []; first=True
        for m in msgs:
            cap = limpiar_otras_etiquetas(m.caption or "", tag) if first else ""
            first = False
            if m.photo:
                media.append(InputMediaPhoto(m.photo.file_id, caption=cap))
            elif m.video:
                media.append(InputMediaVideo(m.video.file_id, caption=cap))
            elif m.document:
                media.append(InputMediaDocument(m.document.file_id, caption=cap))
            elif m.audio:
                media.append(InputMediaAudio(m.audio.file_id, caption=cap))
        if media:
            try:
                bot.send_media_group(DISCUSSION_ID, media=media, reply_to_message_id=gid)
            except RPCError:
                pass
    del albums_in_progress[mgid]

def reset_album_timer(mgid):
    if albums_in_progress[mgid]["timer"]:
        albums_in_progress[mggid]["timer"].cancel()
    t = threading.Timer(ALBUM_TIMEOUT, finalize_album, [mgid])
    albums_in_progress[mggid]["timer"] = t; t.start()

@bot.on_message(filters.private)
def on_private_message(client: Client, message: Message):
    uid = message.from_user.id
    if message.photo or message.video or message.document or message.audio:
        tags = extraer_etiquetas(message.caption or message.text or "")
        if not tags:
            group_sessions[uid].append(message)
            cnt = len(group_sessions[uid])
            return message.reply(
                f"✅ Medio agregado a la sesión (total: {cnt}).\n"
                "Usa /etiquetar #etiqueta o /cancelar cuando quieras."
            )
    txt = message.caption or message.text or ""
    found = extraer_etiquetas(txt)
    if not found:
        return message.reply("No encontré etiquetas.")
    for tag in found:
        gid = asegurar_etiqueta_y_espello(tag)
        if not gid:
            continue
        cap = limpiar_otras_etiquetas(txt, tag)
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
        except RPCError:
            pass
    message.reply(f"Publicado en {len(found)} etiqueta(s).")

if __name__ == "__main__":
    bot.run()
