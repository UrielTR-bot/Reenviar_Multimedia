import logging
import time
import threading
import re
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
api_id = 28651675            # Reemplaza con tu API ID
api_hash = "6438bee32a12da56706170b8f34fb487"   # Reemplaza con tu API HASH
bot_token = "7024044929:AAFy-R7CCDfHuRI9Y6ZGm6rA9tP7lgeaFvI" # Reemplaza con tu token del bot

CHANNEL_ID = -1002169066047      # ID numérico de tu canal
DISCUSSION_ID = -1002190873277   # ID numérico del grupo de discusión vinculado

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Client("bot_discussion", api_id=api_id, api_hash=api_hash, bot_token=bot_token)

# Diccionario para mapear etiqueta -> ID del mensaje espejo en el grupo
etiquetas = {}

# Diccionario para mapear channel_msg_id -> etiqueta(s) pendientes
# (cuando creamos un post en el canal, esperamos el "mensaje espejo" en el grupo)
pending_mirrors = {}

# Manejo de álbumes (media_group_id) en progreso
# Ahora almacenamos un set de etiquetas, en lugar de una sola "etiqueta"
albums_in_progress = defaultdict(lambda: {
    "messages": [],
    "timer": None,
    "etiquetas": set()
})
ALBUM_TIMEOUT = 10  # segundos de espera para “cerrar” un álbum

# ----------------------------
# Función para extraer TODAS las etiquetas de un texto
# ----------------------------
def extraer_etiquetas(texto: str) -> set:
    """
    Retorna un conjunto de todas las etiquetas (#algo) encontradas en 'texto'.
    Por ejemplo, "#tag1 #tag2" => {"#tag1", "#tag2"}.
    """
    pattern = r"#\w+"
    return set(re.findall(pattern, texto or ""))

# ----------------------------
# 1) Capturar el mensaje espejo en el grupo
# ----------------------------
@bot.on_message(filters.chat(DISCUSSION_ID))
def on_group_message(client: Client, message: Message):
    """
    Escuchamos todos los mensajes en el grupo de discusión.
    Cuando se crea el "mensaje espejo" de un post del canal:
      forward_from_message_id == ID del post en el canal
    Asignamos ese message_id al diccionario etiquetas[etiqueta].
    """
    if not hasattr(message, "forward_from_message_id"):
        return

    fwd_id = message.forward_from_message_id
    if fwd_id in pending_mirrors:
        # Tomamos la etiqueta que estaba pendiente
        etiqueta = pending_mirrors[fwd_id]
        etiquetas[etiqueta] = message.id
        logger.info(f"Mensaje espejo para etiqueta {etiqueta} capturado: group_msg_id={message.id}")
        del pending_mirrors[fwd_id]

# ----------------------------
# 2) Manejo de álbumes
# ----------------------------
def finalize_album(media_group_id):
    """
    Llamado cuando creemos que ya llegaron todos los mensajes del álbum (tras ALBUM_TIMEOUT).
    Lo enviamos como un solo bloque (send_media_group) al grupo,
    respondiendo al mensaje espejo de CADA etiqueta detectada.
    """
    data = albums_in_progress[media_group_id]
    msgs = data["messages"]
    all_tags = data["etiquetas"]  # set de todas las etiquetas encontradas

    # Ordenar por fecha
    msgs.sort(key=lambda x: x.date)

    # Si no se detectó ninguna etiqueta, ignoramos
    if not all_tags:
        logger.info(f"Álbum {media_group_id} sin etiquetas. Se ignora.")
        del albums_in_progress[media_group_id]
        return

    logger.info(f"Finalizando álbum {media_group_id} con {len(msgs)} msgs y etiquetas={all_tags}")

    # Construir el media_group
    media = []
    first = True
    for am in msgs:
        cap = am.caption if (first and am.caption) else ""
        first = False
        if am.photo:
            media.append(InputMediaPhoto(am.photo.file_id, caption=cap))
        elif am.video:
            media.append(InputMediaVideo(am.video.file_id, caption=cap))
        elif am.document:
            media.append(InputMediaDocument(am.document.file_id, caption=cap))
        elif am.audio:
            media.append(InputMediaAudio(am.audio.file_id, caption=cap))

    if not media:
        logger.info(f"Álbum {media_group_id} no tiene archivos válidos.")
        del albums_in_progress[media_group_id]
        return

    # Enviar el mismo álbum en CADA etiqueta
    for tag in all_tags:
        group_msg_id = asegurar_etiqueta_y_espelho(tag)
        if not group_msg_id:
            logger.error(f"No se pudo obtener espejo para {tag}. Se omite.")
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

# ----------------------------
# 3) Crear post en canal y registrar "mensaje espejo" pendiente
# ----------------------------
def asegurar_etiqueta_y_espelho(etiqueta: str) -> int:
    """
    Si la etiqueta ya tiene un group_msg_id, lo retornamos.
    Si no, creamos un post en el canal y registramos pending_mirrors[post_id] = etiqueta.
    Luego, cuando llegue el "mensaje espejo" al grupo, on_group_message lo asignará.
    Esperamos hasta 10s a ver si se registra.
    """
    if etiqueta in etiquetas:
        return etiquetas[etiqueta]

    # Crear post en el canal
    post = bot.send_message(CHANNEL_ID, etiqueta)
    post_id = post.id
    logger.info(f"Post creado en canal para {etiqueta} => channel_msg_id={post_id}")

    # Registrar en pending_mirrors
    pending_mirrors[post_id] = etiqueta

    # Esperar un tiempo a que llegue el update del espejo (polling 10s)
    start = time.time()
    while True:
        if etiqueta in etiquetas:
            return etiquetas[etiqueta]
        if time.time() - start > 10:
            break
        time.sleep(1)

    logger.error(f"No llegó el espejo para {etiqueta} tras 10s.")
    return 0

# ----------------------------
# 4) Handler de mensajes privados
# ----------------------------
@bot.on_message(filters.private)
def on_private_message(client: Client, message: Message):
    """
    Cuando reenvías/mandas un mensaje o álbum con #etiqueta1 #etiqueta2 ...
    al bot en privado:
    - Si es parte de un álbum, lo bufferizamos.
    - Si es un mensaje suelto, lo copiamos al grupo en CADA etiqueta detectada.
    """
    if message.media_group_id:
        # Es parte de un álbum
        mgid = message.media_group_id
        albums_in_progress[mgid]["messages"].append(message)

        # Extraer TODAS las etiquetas del caption/texto
        texto = (message.caption or message.text or "")
        found_tags = extraer_etiquetas(texto)
        albums_in_progress[mgid]["etiquetas"].update(found_tags)

        reset_album_timer(mgid)

    else:
        # Mensaje suelto
        texto = (message.caption or message.text or "")
        found_tags = extraer_etiquetas(texto)
        if not found_tags:
            message.reply("No encontré ninguna etiqueta (#).")
            return

        logger.info(f"Mensaje suelto con etiquetas: {found_tags}")

        # Publicar en CADA etiqueta
        for tag in found_tags:
            group_msg_id = asegurar_etiqueta_y_espelho(tag)
            if not group_msg_id:
                message.reply(f"No se pudo obtener espejo para {tag}. Revisa permisos.")
                continue

            # Copiar mensaje al grupo como comentario
            try:
                reenviado = bot.copy_message(
                    chat_id=DISCUSSION_ID,
                    from_chat_id=message.chat.id,
                    message_id=message.id,
                    reply_to_message_id=group_msg_id
                )
                logger.info(f"Mensaje {message.id} -> etiqueta {tag}, new_id={reenviado.id}")
            except RPCError as e:
                logger.error(f"Error al reenviar msg {message.id} a {tag}: {e}")

        message.reply(f"¡Listo! Tu mensaje se publicó en {len(found_tags)} etiqueta(s).")

# ----------------------------
# Iniciar el bot
# ----------------------------
bot.run()
