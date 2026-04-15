"""
bot2_handler.py — @paypix_nexbot
Bot Telegram 100% independente do Bot 1.
• python-telegram-bot v20
• Mercado Pago PIX (depósito + saque manual)
• Cria canal próprio no Telegram (notificações + histórico)
• Banco: tabelas mp2_* no PostgreSQL
"""
import os
import asyncio
import logging
from datetime import datetime
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ChatPermissions
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ConversationHandler, filters,
    ContextTypes
)
from telegram.error import TelegramError, BadRequest, Forbidden
from mp2_api import (
    mp2_get_ou_criar_usuario, mp2_get_saldo, mp2_gerar_pix,
    mp2_solicitar_saque, mp2_historico, mp2_get_config,
    mp2_stats_admin as mp2_stats, init_mp2_db,
    mp2_salvar_canal, mp2_get_canal
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [Bot2] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

BOT2_TOKEN    = os.environ.get('BOT2_TOKEN', '')
BOT2_ADMIN_ID = int(os.environ.get('BOT2_ADMIN_ID', '0'))  # Telegram ID do dono

# ─── ESTADOS ConversationHandler ─────────────────────────────────────────────
(
    AGUARDANDO_VALOR_DEP,
    AGUARDANDO_CHAVE_PIX,
    AGUARDANDO_VALOR_SAQUE,
    AGUARDANDO_TIPO_CHAVE,
    AGUARDANDO_CONFIRMACAO_SAQUE,
) = range(5)

# ─── TECLADO PRINCIPAL ────────────────────────────────────────────────────────
def teclado_principal():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("💰 Depositar"),  KeyboardButton("💼 Carteira")],
            [KeyboardButton("📤 Sacar"),      KeyboardButton("📊 Histórico")],
            [KeyboardButton("👥 Indicar"),    KeyboardButton("ℹ️ Ajuda")],
        ],
        resize_keyboard=True
    )

def teclado_cancelar():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("❌ Cancelar")]],
        resize_keyboard=True
    )

# ─── FORMATAR VALOR ──────────────────────────────────────────────────────────
def fmt(v):
    return f"R$ {float(v):.2f}".replace('.', ',')

# ─── /start ──────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args

    referido_por = None
    if args and args[0].startswith('ref_'):
        try:
            referido_por = int(args[0].replace('ref_', ''))
        except:
            pass

    usuario = mp2_get_ou_criar_usuario(
        telegram_id=user.id,
        username=user.username or '',
        nome=user.full_name or '',
        referido_por=referido_por
    )

    saldo = float(usuario.get('saldo', 0))
    nome  = usuario.get('nome', user.first_name or 'usuário')

    cfg         = mp2_get_config()
    dep_min     = cfg.get('deposito_minimo', '5')
    saque_min   = cfg.get('saque_minimo', '20')
    comissao    = cfg.get('comissao_pct', '5')

    # Link do canal
    canal = mp2_get_canal('notificacoes')
    canal_link = f"\n📣 [Acompanhe no canal]({canal['invite_link']})" if canal else ""

    texto = (
        f"🏦 *Bem-vindo ao PayPixNex!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Olá *{nome}*! 👋\n\n"
        f"💰 *Seu saldo:* {fmt(saldo)}\n\n"
        f"📋 *Como funciona:*\n"
        f"• Deposite via PIX instantâneo\n"
        f"• Saque para qualquer chave PIX\n"
        f"• Indique amigos e ganhe *{comissao}%* de comissão\n\n"
        f"📌 *Limites:*\n"
        f"• Depósito mínimo: R$ {dep_min}\n"
        f"• Saque mínimo: R$ {saque_min}\n"
        f"{canal_link}\n\n"
        f"_Use os botões abaixo para começar:_"
    )

    await update.message.reply_text(
        texto,
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── CARTEIRA ────────────────────────────────────────────────────────────────
async def cmd_carteira(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    usuario = mp2_get_ou_criar_usuario(user.id, user.username or '', user.full_name or '')

    saldo       = float(usuario.get('saldo', 0))
    total_dep   = float(usuario.get('total_depositado', 0))
    total_saq   = float(usuario.get('total_sacado', 0))
    criado_em   = usuario.get('criado_em', '')
    if criado_em:
        try:
            criado_em = datetime.fromisoformat(str(criado_em)).strftime('%d/%m/%Y')
        except:
            criado_em = str(criado_em)[:10]

    cfg       = mp2_get_config()
    saque_min = cfg.get('saque_minimo', '20')

    texto = (
        f"💼 *Sua Carteira*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 *Saldo disponível:* {fmt(saldo)}\n\n"
        f"📥 Total depositado: {fmt(total_dep)}\n"
        f"📤 Total sacado:     {fmt(total_saq)}\n\n"
        f"📌 Saque mínimo: R$ {saque_min}\n"
        f"🗓 Conta criada em: {criado_em}\n\n"
        f"_Para sacar, use o botão_ 📤 *Sacar*"
    )

    btn = InlineKeyboardMarkup([[
        InlineKeyboardButton("💰 Depositar agora", callback_data="dep_novo"),
        InlineKeyboardButton("📤 Sacar",           callback_data="saq_novo"),
    ]])

    await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=btn)

# ─── DEPOSITAR ───────────────────────────────────────────────────────────────
async def cmd_depositar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg     = mp2_get_config()
    dep_min = cfg.get('deposito_minimo', '5')
    dep_max = cfg.get('deposito_maximo', '10000')

    await update.message.reply_text(
        f"💰 *Novo Depósito via PIX*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Digite o valor que deseja depositar:\n"
        f"• Mínimo: R$ {dep_min}\n"
        f"• Máximo: R$ {dep_max}\n\n"
        f"_Ex: 50 ou 150.00_",
        parse_mode='Markdown',
        reply_markup=teclado_cancelar()
    )
    return AGUARDANDO_VALOR_DEP

async def receber_valor_dep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    texto = update.message.text.strip()

    if texto == "❌ Cancelar":
        return await cmd_cancelar(update, context)

    try:
        valor = float(texto.replace(',', '.').replace('R$', '').strip())
    except:
        await update.message.reply_text(
            "❌ Valor inválido. Digite um número válido.\n_Ex: 50 ou 150.00_",
            parse_mode='Markdown'
        )
        return AGUARDANDO_VALOR_DEP

    cfg     = mp2_get_config()
    dep_min = float(cfg.get('deposito_minimo', '5'))
    dep_max = float(cfg.get('deposito_maximo', '10000'))

    if valor < dep_min:
        await update.message.reply_text(
            f"❌ Valor mínimo é R$ {dep_min:.2f}".replace('.', ',')
        )
        return AGUARDANDO_VALOR_DEP

    if valor > dep_max:
        await update.message.reply_text(
            f"❌ Valor máximo é R$ {dep_max:.2f}".replace('.', ',')
        )
        return AGUARDANDO_VALOR_DEP

    # Gera PIX via Mercado Pago
    msg_aguarde = await update.message.reply_text(
        "⏳ Gerando PIX... aguarde alguns segundos.",
        reply_markup=teclado_cancelar()
    )

    resultado = mp2_gerar_pix(
        telegram_id=user.id,
        valor=valor,
        descricao=f"Depósito PayPixNex @{user.username or user.id}"
    )

    if not resultado.get('success'):
        await msg_aguarde.edit_text(
            f"❌ Erro ao gerar PIX: {resultado.get('error', 'tente novamente')}\n\n"
            f"_Verifique se o token MP2_ACCESS_TOKEN está configurado._",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    pix_code = resultado.get('pix_copia_cola', '')
    qr_b64   = resultado.get('pix_qr_base64', '')
    ext_ref  = resultado.get('external_ref', '')
    expires  = resultado.get('expiracao', '30 minutos')

    context.user_data['ultimo_dep'] = {'valor': valor, 'ext_ref': ext_ref}

    btn = InlineKeyboardMarkup([[
        InlineKeyboardButton("📋 Copiei o código!", callback_data=f"dep_copiado_{ext_ref[:20]}"),
        InlineKeyboardButton("✅ Já paguei",        callback_data=f"dep_pago_{ext_ref[:20]}"),
    ]])

    # Envia QR code se disponível
    if qr_b64:
        try:
            import base64
            from io import BytesIO
            img_bytes = base64.b64decode(qr_b64)
            await update.message.reply_photo(
                photo=BytesIO(img_bytes),
                caption=f"📱 *QR Code — {fmt(valor)}*\nEscaneie com seu app de banco",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.warning(f"Erro ao enviar QR: {e}")

    await msg_aguarde.edit_text(
        f"✅ *PIX Gerado!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 *Valor:* {fmt(valor)}\n"
        f"⏰ *Expira em:* {expires}\n\n"
        f"📋 *Código PIX (Copia e Cola):*\n"
        f"`{pix_code}`\n\n"
        f"_Após pagar, seu saldo é atualizado automaticamente!_",
        parse_mode='Markdown',
        reply_markup=btn
    )

    await update.message.reply_text(
        "🏠 Menu principal:",
        reply_markup=teclado_principal()
    )
    return ConversationHandler.END

# ─── SACAR ───────────────────────────────────────────────────────────────────
async def cmd_sacar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    usuario = mp2_get_ou_criar_usuario(user.id, user.username or '', user.full_name or '')
    saldo   = float(usuario.get('saldo', 0))
    cfg     = mp2_get_config()
    saque_min = float(cfg.get('saque_minimo', '20'))

    if saldo < saque_min:
        await update.message.reply_text(
            f"❌ *Saldo insuficiente*\n\n"
            f"💰 Seu saldo: {fmt(saldo)}\n"
            f"📌 Mínimo para saque: {fmt(saque_min)}\n\n"
            f"_Deposite mais para poder sacar._",
            parse_mode='Markdown',
            reply_markup=teclado_principal()
        )
        return ConversationHandler.END

    await update.message.reply_text(
        f"📤 *Solicitar Saque*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 Saldo disponível: {fmt(saldo)}\n"
        f"📌 Mínimo: {fmt(saque_min)}\n\n"
        f"Digite o valor que deseja sacar:",
        parse_mode='Markdown',
        reply_markup=teclado_cancelar()
    )
    return AGUARDANDO_VALOR_SAQUE

async def receber_valor_saque(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user  = update.effective_user
    texto = update.message.text.strip()

    if texto == "❌ Cancelar":
        return await cmd_cancelar(update, context)

    try:
        valor = float(texto.replace(',', '.').replace('R$', '').strip())
    except:
        await update.message.reply_text("❌ Valor inválido. Digite um número.")
        return AGUARDANDO_VALOR_SAQUE

    usuario  = mp2_get_ou_criar_usuario(user.id, user.username or '', user.full_name or '')
    saldo    = float(usuario.get('saldo', 0))
    cfg      = mp2_get_config()
    saque_min = float(cfg.get('saque_minimo', '20'))

    if valor < saque_min:
        await update.message.reply_text(f"❌ Mínimo para saque: {fmt(saque_min)}")
        return AGUARDANDO_VALOR_SAQUE

    if valor > saldo:
        await update.message.reply_text(
            f"❌ Saldo insuficiente.\n💰 Seu saldo: {fmt(saldo)}"
        )
        return AGUARDANDO_VALOR_SAQUE

    context.user_data['saque_valor'] = valor

    btn = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📱 CPF",      callback_data="tipo_cpf"),
            InlineKeyboardButton("✉️ E-mail",   callback_data="tipo_email"),
        ],
        [
            InlineKeyboardButton("📞 Telefone", callback_data="tipo_telefone"),
            InlineKeyboardButton("🔑 Aleatória",callback_data="tipo_aleatoria"),
        ],
    ])

    await update.message.reply_text(
        f"📤 *Saque de {fmt(valor)}*\n\n"
        f"Selecione o tipo da sua chave PIX:",
        parse_mode='Markdown',
        reply_markup=btn
    )
    return AGUARDANDO_TIPO_CHAVE

async def receber_tipo_chave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    tipo_map = {
        'tipo_cpf':       ('CPF',       'cpf'),
        'tipo_email':     ('E-mail',    'email'),
        'tipo_telefone':  ('Telefone',  'telefone'),
        'tipo_aleatoria': ('Aleatória', 'aleatoria'),
    }

    data = query.data
    if data not in tipo_map:
        return AGUARDANDO_TIPO_CHAVE

    label, tipo = tipo_map[data]
    context.user_data['saque_tipo_chave'] = tipo

    exemplos = {
        'cpf':       '_Ex: 123.456.789-00_',
        'email':     '_Ex: seuemail@gmail.com_',
        'telefone':  '_Ex: +5511999887766_',
        'aleatoria': '_Ex: 123e4567-e89b-..._',
    }

    await query.edit_message_text(
        f"🔑 *Chave PIX ({label})*\n\n"
        f"Digite sua chave PIX:\n{exemplos.get(tipo, '')}",
        parse_mode='Markdown'
    )
    return AGUARDANDO_CHAVE_PIX

async def receber_chave_pix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user  = update.effective_user
    chave = update.message.text.strip()

    if chave == "❌ Cancelar":
        return await cmd_cancelar(update, context)

    valor      = context.user_data.get('saque_valor', 0)
    tipo_chave = context.user_data.get('saque_tipo_chave', 'aleatoria')
    context.user_data['saque_chave'] = chave

    btn = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirmar Saque", callback_data="saque_confirmar"),
        InlineKeyboardButton("❌ Cancelar",        callback_data="saque_cancelar"),
    ]])

    await update.message.reply_text(
        f"📤 *Confirmar Saque*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 Valor: *{fmt(valor)}*\n"
        f"🔑 Chave: `{chave}`\n"
        f"📌 Tipo: {tipo_chave.upper()}\n\n"
        f"⚠️ _Saques são processados manualmente em até 24h._\n\n"
        f"Confirma?",
        parse_mode='Markdown',
        reply_markup=btn
    )
    return AGUARDANDO_CONFIRMACAO_SAQUE

async def confirmar_saque(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user  = update.effective_user

    if query.data == 'saque_cancelar':
        await query.edit_message_text("❌ Saque cancelado.")
        await context.bot.send_message(
            user.id, "🏠 Operação cancelada.",
            reply_markup=teclado_principal()
        )
        return ConversationHandler.END

    valor      = context.user_data.get('saque_valor', 0)
    chave      = context.user_data.get('saque_chave', '')
    tipo_chave = context.user_data.get('saque_tipo_chave', 'aleatoria')

    resultado = mp2_solicitar_saque(
        telegram_id=user.id,
        valor=valor,
        chave_pix=chave,
        tipo_chave=tipo_chave
    )

    if resultado.get('success'):
        saque_id = resultado.get('saque_id', '?')

        await query.edit_message_text(
            f"✅ *Saque Solicitado!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 Protocolo: `#{saque_id}`\n"
            f"💰 Valor: *{fmt(valor)}*\n"
            f"🔑 Chave: `{chave}`\n\n"
            f"⏳ *Prazo:* até 24 horas úteis\n"
            f"📬 Você receberá uma notificação aqui quando for processado.",
            parse_mode='Markdown'
        )

        # Notifica o canal sobre o novo saque
        asyncio.create_task(
            _notificar_canal_saque(context.bot, user, valor, saque_id)
        )
    else:
        await query.edit_message_text(
            f"❌ Erro: {resultado.get('error', 'tente novamente')}"
        )

    await context.bot.send_message(
        user.id, "🏠 Menu principal:",
        reply_markup=teclado_principal()
    )
    return ConversationHandler.END

async def _notificar_canal_saque(bot, user, valor, saque_id):
    """Notifica o canal sobre saque solicitado."""
    try:
        canal = mp2_get_canal('notificacoes')
        if not canal:
            return
        canal_id = canal['canal_id']
        username = f"@{user.username}" if user.username else user.full_name
        msg = (
            f"📤 *Saque Solicitado*\n"
            f"👤 {username}\n"
            f"💰 Valor: *{fmt(valor)}*\n"
            f"🆔 #{saque_id}\n"
            f"⏳ Aguardando processamento"
        )
        await bot.send_message(canal_id, msg, parse_mode='Markdown')
    except Exception as e:
        logger.warning(f"Erro notificar canal saque: {e}")

# ─── HISTÓRICO ───────────────────────────────────────────────────────────────
async def cmd_historico(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    historico = mp2_historico(user.id, limite=10)

    if not historico:
        await update.message.reply_text(
            "📊 *Histórico vazio*\n\nVocê ainda não tem transações.",
            parse_mode='Markdown',
            reply_markup=teclado_principal()
        )
        return

    linhas = ["📊 *Últimas Transações*\n━━━━━━━━━━━━━━━━━━━━━\n"]
    for tx in historico:
        tipo   = tx.get('tipo', '')
        valor  = float(tx.get('valor', 0))
        status = tx.get('status', '')
        data   = tx.get('criado_em', '')
        if data:
            try:
                data = datetime.fromisoformat(str(data)).strftime('%d/%m %H:%M')
            except:
                data = str(data)[:16]

        emoji_tipo   = "📥" if tipo == 'deposito' else "📤" if tipo == 'saque' else "🎁"
        emoji_status = {"confirmado":"✅","pendente":"⏳","cancelado":"❌"}.get(status, "❓")

        linhas.append(
            f"{emoji_tipo} {fmt(valor)} {emoji_status} _{data}_"
        )

    await update.message.reply_text(
        "\n".join(linhas),
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── INDICAR ─────────────────────────────────────────────────────────────────
async def cmd_indicar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot_info = await context.bot.get_me()
    bot_username = bot_info.username

    cfg      = mp2_get_config()
    comissao = cfg.get('comissao_pct', '5')

    link = f"https://t.me/{bot_username}?start=ref_{user.id}"

    await update.message.reply_text(
        f"👥 *Programa de Indicação*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Ganhe *{comissao}%* de cada depósito dos seus indicados!\n\n"
        f"🔗 *Seu link exclusivo:*\n"
        f"`{link}`\n\n"
        f"_Compartilhe e ganhe automaticamente!_",
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── AJUDA ───────────────────────────────────────────────────────────────────
async def cmd_ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    canal = mp2_get_canal('notificacoes')
    canal_txt = f"\n📣 [Canal de Notificações]({canal['invite_link']})" if canal else ""

    await update.message.reply_text(
        f"ℹ️ *Ajuda — PayPixNex*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"*Como depositar:*\n"
        f"1. Clique em 💰 Depositar\n"
        f"2. Digite o valor desejado\n"
        f"3. Copie o código PIX e pague\n"
        f"4. Saldo atualiza automaticamente ✅\n\n"
        f"*Como sacar:*\n"
        f"1. Clique em 📤 Sacar\n"
        f"2. Informe o valor e a chave PIX\n"
        f"3. Aguarde até 24h para processamento\n\n"
        f"*Suporte:* @paypix_nexbot{canal_txt}\n\n"
        f"_Dúvidas? Envie /start para recomeçar._",
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── /admin — comando do dono do bot ─────────────────────────────────────────
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if BOT2_ADMIN_ID and user.id != BOT2_ADMIN_ID:
        await update.message.reply_text("❌ Acesso negado.")
        return

    stats = mp2_stats()
    await update.message.reply_text(
        f"🔧 *Painel Admin — @paypix_nexbot*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Usuários: {stats.get('total_usuarios', 0)}\n"
        f"📥 Depósitos confirmados: {stats.get('depositos_confirmados', 0)}\n"
        f"💰 Total depositado: {fmt(stats.get('total_depositado', 0))}\n"
        f"📤 Saques pendentes: {stats.get('saques_pendentes', 0)}\n"
        f"💸 Valor saques pend.: {fmt(stats.get('valor_saques_pendentes', 0))}\n"
        f"✅ Saques aprovados: {stats.get('saques_aprovados', 0)}\n\n"
        f"_Use /criar\\_canal para criar/recriar o canal_",
        parse_mode='Markdown'
    )

# ─── /criar_canal — cria canal de notificações ───────────────────────────────
async def cmd_criar_canal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if BOT2_ADMIN_ID and user.id != BOT2_ADMIN_ID:
        await update.message.reply_text("❌ Acesso negado.")
        return

    msg = await update.message.reply_text("⏳ Criando canal PayPixNex...")

    resultado = await criar_canal_notificacoes(context.bot)

    if resultado.get('success'):
        link = resultado.get('invite_link', '')
        await msg.edit_text(
            f"✅ *Canal criado com sucesso!*\n\n"
            f"📣 Nome: PayPixNex Notificações\n"
            f"🔗 Link: {link}\n\n"
            f"_Compartilhe para que os usuários possam acompanhar!_",
            parse_mode='Markdown'
        )
    else:
        await msg.edit_text(
            f"❌ Erro ao criar canal: {resultado.get('error', 'desconhecido')}\n\n"
            f"_Certifique-se que o BOT2_TOKEN está correto e o bot tem permissão._"
        )

# ─── CRIAR CANAL AUTOMATICAMENTE ─────────────────────────────────────────────
async def criar_canal_notificacoes(bot) -> dict:
    """
    Cria o canal 'PayPixNex Notificações' automaticamente.
    Salva canal_id e invite_link no PostgreSQL (mp2_config).
    """
    try:
        # Verifica se já existe
        canal_existente = mp2_get_canal('notificacoes')
        if canal_existente:
            # Testa se o canal ainda existe
            try:
                await bot.get_chat(canal_existente['canal_id'])
                logger.info(f"[Bot2] Canal já existe: {canal_existente['canal_id']}")
                return {'success': True, **canal_existente}
            except Exception:
                logger.info("[Bot2] Canal anterior inválido, recriando...")

        # Cria novo canal
        canal = await bot.create_chat(
            title="📣 PayPixNex Notificações",
            chat_type="channel"
        )
        canal_id = canal.id

        # Gera link de convite
        invite_link = await bot.export_chat_invite_link(canal_id)

        # Salva no banco
        mp2_salvar_canal('notificacoes', canal_id, invite_link, "📣 PayPixNex Notificações")

        # Mensagem de boas-vindas
        await bot.send_message(
            canal_id,
            f"🚀 *Canal PayPixNex ativo!*\n\n"
            f"📣 Aqui você acompanha:\n"
            f"• ✅ PIX confirmados\n"
            f"• 📤 Saques processados\n"
            f"• 📊 Atualizações do sistema\n\n"
            f"_Bem-vindo ao PayPixNex!_ 🏦",
            parse_mode='Markdown'
        )

        logger.info(f"[Bot2] ✅ Canal criado: {canal_id} — {invite_link}")
        return {'success': True, 'canal_id': canal_id, 'invite_link': invite_link}

    except Exception as e:
        logger.error(f"[Bot2] Erro criar canal: {e}")
        return {'success': False, 'error': str(e)}

# ─── CALLBACK HANDLER ────────────────────────────────────────────────────────
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "dep_novo":
        await query.message.reply_text(
            "💰 Digite o valor para depositar:",
            reply_markup=teclado_cancelar()
        )
        return AGUARDANDO_VALOR_DEP

    if data == "saq_novo":
        await query.message.reply_text(
            "📤 Digite o valor para sacar:",
            reply_markup=teclado_cancelar()
        )
        return AGUARDANDO_VALOR_SAQUE

    if data.startswith("dep_copiado_") or data.startswith("dep_pago_"):
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            "✅ Ótimo! Assim que o pagamento for confirmado pelo Mercado Pago, "
            "seu saldo será atualizado automaticamente!\n\n"
            "_Isso leva alguns segundos após o pagamento._",
            reply_markup=teclado_principal()
        )

# ─── HANDLER TEXTO LIVRE ─────────────────────────────────────────────────────
async def handler_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text.strip()

    rota = {
        "💰 Depositar": cmd_depositar,
        "💼 Carteira":  cmd_carteira,
        "📤 Sacar":     cmd_sacar,
        "📊 Histórico": cmd_historico,
        "👥 Indicar":   cmd_indicar,
        "ℹ️ Ajuda":    cmd_ajuda,
    }

    if texto in rota:
        return await rota[texto](update, context)

    await update.message.reply_text(
        "❓ Comando não reconhecido. Use os botões abaixo:",
        reply_markup=teclado_principal()
    )

# ─── CANCELAR ────────────────────────────────────────────────────────────────
async def cmd_cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "❌ Operação cancelada.",
        reply_markup=teclado_principal()
    )
    return ConversationHandler.END

# ─── CONSTRUIR APLICAÇÃO ─────────────────────────────────────────────────────
def build_bot2_app():
    if not BOT2_TOKEN:
        logger.warning("[Bot2] BOT2_TOKEN não configurado — bot2 desativado")
        return None

    app = Application.builder().token(BOT2_TOKEN).build()

    # ConversationHandler — depósito
    conv_deposito = ConversationHandler(
        entry_points=[
            CommandHandler('depositar', cmd_depositar),
            MessageHandler(filters.Regex(r'^💰 Depositar$'), cmd_depositar),
            CallbackQueryHandler(cmd_depositar, pattern='^dep_novo$'),
        ],
        states={
            AGUARDANDO_VALOR_DEP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receber_valor_dep)
            ],
        },
        fallbacks=[
            CommandHandler('cancelar', cmd_cancelar),
            MessageHandler(filters.Regex(r'^❌ Cancelar$'), cmd_cancelar),
        ],
        allow_reentry=True,
        per_message=False,
    )

    # ConversationHandler — saque
    conv_saque = ConversationHandler(
        entry_points=[
            CommandHandler('sacar', cmd_sacar),
            MessageHandler(filters.Regex(r'^📤 Sacar$'), cmd_sacar),
            CallbackQueryHandler(cmd_sacar, pattern='^saq_novo$'),
        ],
        states={
            AGUARDANDO_VALOR_SAQUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receber_valor_saque)
            ],
            AGUARDANDO_TIPO_CHAVE: [
                CallbackQueryHandler(receber_tipo_chave, pattern='^tipo_')
            ],
            AGUARDANDO_CHAVE_PIX: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receber_chave_pix)
            ],
            AGUARDANDO_CONFIRMACAO_SAQUE: [
                CallbackQueryHandler(confirmar_saque, pattern='^saque_')
            ],
        },
        fallbacks=[
            CommandHandler('cancelar', cmd_cancelar),
            MessageHandler(filters.Regex(r'^❌ Cancelar$'), cmd_cancelar),
        ],
        allow_reentry=True,
        per_message=False,
    )

    # Registrar handlers
    app.add_handler(CommandHandler('start',        cmd_start))
    app.add_handler(CommandHandler('carteira',     cmd_carteira))
    app.add_handler(CommandHandler('historico',    cmd_historico))
    app.add_handler(CommandHandler('indicar',      cmd_indicar))
    app.add_handler(CommandHandler('ajuda',        cmd_ajuda))
    app.add_handler(CommandHandler('admin',        cmd_admin))
    app.add_handler(CommandHandler('criar_canal',  cmd_criar_canal))
    app.add_handler(conv_deposito)
    app.add_handler(conv_saque)
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handler_texto))

    return app

# ─── STARTUP: CRIAR CANAL AUTOMATICAMENTE ────────────────────────────────────
async def _bot2_post_startup(app: Application):
    """Executado após o bot iniciar: cria canal se não existir."""
    await asyncio.sleep(5)  # aguarda bot conectar
    logger.info("[Bot2] Verificando canal de notificações...")
    resultado = await criar_canal_notificacoes(app.bot)
    if resultado.get('success'):
        logger.info(f"[Bot2] ✅ Canal OK: {resultado.get('invite_link', '')}")
    else:
        logger.warning(f"[Bot2] ⚠️ Canal: {resultado.get('error', '')}")

# ─── INICIAR POLLING ─────────────────────────────────────────────────────────
async def iniciar_bot2_polling():
    """Inicia o bot em modo polling (separado do aiohttp server)."""
    if not BOT2_TOKEN:
        logger.warning("[Bot2] BOT2_TOKEN não definido — polling não iniciado")
        return

    logger.info("[Bot2] Iniciando @paypix_nexbot polling...")
    app = build_bot2_app()
    if not app:
        return

    # Inicializa DB
    init_mp2_db()

    try:
        await app.initialize()
        await app.start()

        # Criar canal após iniciar
        asyncio.create_task(_bot2_post_startup(app))

        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
        logger.info("[Bot2] ✅ Polling ativo — @paypix_nexbot")

        # Manter rodando enquanto o servidor estiver ativo
        while True:
            await asyncio.sleep(60)

    except asyncio.CancelledError:
        logger.info("[Bot2] Polling encerrado (cancelado)")
    except Exception as e:
        logger.error(f"[Bot2] Erro polling: {e}")
    finally:
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except:
            pass
