"""
bot2_handler.py — Bot Telegram @paypix_nexbot
Bot 100% independente do Bot 1 (userbot Telethon).
Usa python-telegram-bot v20 + Mercado Pago PIX.
Banco: tabelas mp2_* (independentes) no mesmo PostgreSQL.
"""
import os
import asyncio
import logging
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ConversationHandler, filters,
    ContextTypes
)
from mp2_api import (
    mp2_get_ou_criar_usuario, mp2_get_saldo, mp2_gerar_pix,
    mp2_solicitar_saque, mp2_historico, mp2_get_config,
    init_mp2_db
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [Bot2] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

BOT2_TOKEN = os.environ.get('BOT2_TOKEN', '')

# ─── ESTADOS ConversationHandler ──────────────────────────────────────────────
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
            [KeyboardButton("💰 Depositar"), KeyboardButton("💼 Carteira")],
            [KeyboardButton("📤 Sacar"),     KeyboardButton("📊 Histórico")],
            [KeyboardButton("👥 Indicar"),   KeyboardButton("ℹ️ Ajuda")],
        ],
        resize_keyboard=True
    )

def teclado_cancelar():
    return ReplyKeyboardMarkup(
        [[KeyboardButton("❌ Cancelar")]],
        resize_keyboard=True
    )

# ─── /start ──────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args  # ex: /start ref_123456789

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
    nome = usuario.get('nome', user.first_name)

    texto = (
        f"🏦 *Bem-vindo ao PayPixNex!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {nome}\n"
        f"💰 Saldo: *R$ {saldo:.2f}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Escolha uma opção abaixo:"
    )

    await update.message.reply_text(
        texto,
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── CARTEIRA ─────────────────────────────────────────────────────────────────
async def cmd_carteira(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    usuario = mp2_get_ou_criar_usuario(user.id, user.username or '', user.full_name or '')

    saldo = float(usuario.get('saldo', 0))
    total_dep = float(usuario.get('total_depositado', 0))
    total_sac = float(usuario.get('total_sacado', 0))
    telegram_id = user.id

    texto = (
        f"💼 *Sua Carteira*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Saldo atual: *R$ {saldo:.2f}*\n\n"
        f"📊 *Resumo:*\n"
        f"  📥 Total depositado: R$ {total_dep:.2f}\n"
        f"  📤 Total sacado:     R$ {total_sac:.2f}\n"
        f"  🆔 ID: `{telegram_id}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━"
    )

    await update.message.reply_text(
        texto,
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── DEPÓSITO (ConversationHandler) ──────────────────────────────────────────
async def cmd_depositar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dep_min = mp2_get_config('deposito_minimo', '5')
    dep_max = mp2_get_config('deposito_maximo', '10000')

    await update.message.reply_text(
        f"💰 *Depósito via Pix*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Mínimo: R$ {dep_min} | Máximo: R$ {dep_max}\n\n"
        f"Digite o valor que deseja depositar:\n"
        f"_Ex: 50 ou 50.00_",
        parse_mode='Markdown',
        reply_markup=teclado_cancelar()
    )
    return AGUARDANDO_VALOR_DEP

async def receber_valor_dep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text.strip()

    if texto == '❌ Cancelar':
        await update.message.reply_text(
            "❌ Depósito cancelado.",
            reply_markup=teclado_principal()
        )
        return ConversationHandler.END

    try:
        valor = float(texto.replace(',', '.').replace('R$', '').strip())
    except:
        await update.message.reply_text("⚠️ Valor inválido. Digite apenas o número. Ex: 50")
        return AGUARDANDO_VALOR_DEP

    dep_min = float(mp2_get_config('deposito_minimo', '5'))
    dep_max = float(mp2_get_config('deposito_maximo', '10000'))

    if valor < dep_min:
        await update.message.reply_text(f"⚠️ Valor mínimo: R$ {dep_min:.2f}")
        return AGUARDANDO_VALOR_DEP
    if valor > dep_max:
        await update.message.reply_text(f"⚠️ Valor máximo: R$ {dep_max:.2f}")
        return AGUARDANDO_VALOR_DEP

    user = update.effective_user
    mp2_get_ou_criar_usuario(user.id, user.username or '', user.full_name or '')

    await update.message.reply_text(
        f"⏳ Gerando Pix de *R$ {valor:.2f}*... aguarde.",
        parse_mode='Markdown'
    )

    resultado = mp2_gerar_pix(
        telegram_id=user.id,
        valor=valor,
        nome_pagador=user.full_name or 'Cliente'
    )

    if not resultado.get('success'):
        await update.message.reply_text(
            f"❌ {resultado.get('error', 'Erro ao gerar Pix.')}",
            reply_markup=teclado_principal()
        )
        return ConversationHandler.END

    pix_code = resultado.get('pix_copia_cola', '')
    payment_id = resultado.get('payment_id', '')

    # Botão copiar + verificar
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Pix Copia e Cola", callback_data=f"copyfix_{payment_id}")],
        [InlineKeyboardButton("✅ Já paguei!", callback_data=f"checkpix_{payment_id}")],
    ])

    await update.message.reply_text(
        f"✅ *Pix gerado com sucesso!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Valor: *R$ {valor:.2f}*\n"
        f"⏱️ Válido por 30 minutos\n\n"
        f"📋 *Pix Copia e Cola:*\n"
        f"`{pix_code}`\n\n"
        f"_Após pagar, clique em ✅ Já paguei!_",
        parse_mode='Markdown',
        reply_markup=keyboard
    )

    await update.message.reply_text(
        "🏠 Menu principal:",
        reply_markup=teclado_principal()
    )

    return ConversationHandler.END

# ─── SAQUE (ConversationHandler) ──────────────────────────────────────────────
async def cmd_sacar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    saldo = mp2_get_saldo(user.id)
    saque_min = mp2_get_config('saque_minimo', '20')

    await update.message.reply_text(
        f"📤 *Saque Manual*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Saldo disponível: *R$ {saldo:.2f}*\n"
        f"💵 Mínimo: R$ {saque_min}\n\n"
        f"Digite o valor que deseja sacar:",
        parse_mode='Markdown',
        reply_markup=teclado_cancelar()
    )
    return AGUARDANDO_VALOR_SAQUE

async def receber_valor_saque(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text.strip()
    if texto == '❌ Cancelar':
        await update.message.reply_text("❌ Saque cancelado.", reply_markup=teclado_principal())
        return ConversationHandler.END

    try:
        valor = float(texto.replace(',', '.').replace('R$', '').strip())
    except:
        await update.message.reply_text("⚠️ Valor inválido. Ex: 100 ou 100.00")
        return AGUARDANDO_VALOR_SAQUE

    user = update.effective_user
    saldo = mp2_get_saldo(user.id)
    saque_min = float(mp2_get_config('saque_minimo', '20'))

    if valor < saque_min:
        await update.message.reply_text(f"⚠️ Valor mínimo: R$ {saque_min:.2f}")
        return AGUARDANDO_VALOR_SAQUE
    if valor > saldo:
        await update.message.reply_text(
            f"⚠️ Saldo insuficiente!\n"
            f"Saldo: R$ {saldo:.2f} | Solicitado: R$ {valor:.2f}"
        )
        return AGUARDANDO_VALOR_SAQUE

    context.user_data['saque_valor'] = valor

    keyboard = ReplyKeyboardMarkup(
        [
            [KeyboardButton("📱 Celular"), KeyboardButton("📧 E-mail")],
            [KeyboardButton("🪪 CPF"),     KeyboardButton("🔑 Chave aleatória")],
            [KeyboardButton("❌ Cancelar")],
        ],
        resize_keyboard=True
    )

    await update.message.reply_text(
        f"💰 Saque: *R$ {valor:.2f}*\n\n"
        f"Qual tipo de chave Pix?",
        parse_mode='Markdown',
        reply_markup=keyboard
    )
    return AGUARDANDO_TIPO_CHAVE

async def receber_tipo_chave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text.strip()
    if texto == '❌ Cancelar':
        await update.message.reply_text("❌ Saque cancelado.", reply_markup=teclado_principal())
        return ConversationHandler.END

    mapa = {
        "📱 Celular": ("telefone", "Celular (ex: 11999887766)"),
        "📧 E-mail": ("email", "E-mail (ex: nome@email.com)"),
        "🪪 CPF": ("cpf", "CPF (somente números, ex: 12345678901)"),
        "🔑 Chave aleatória": ("aleatoria", "Chave aleatória (cole a chave completa)"),
    }

    tipo_info = mapa.get(texto)
    if not tipo_info:
        await update.message.reply_text("⚠️ Escolha uma das opções acima.")
        return AGUARDANDO_TIPO_CHAVE

    tipo_chave, descricao = tipo_info
    context.user_data['saque_tipo'] = tipo_chave

    await update.message.reply_text(
        f"📝 Digite sua chave Pix ({descricao}):",
        reply_markup=teclado_cancelar()
    )
    return AGUARDANDO_CHAVE_PIX

async def receber_chave_pix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chave = update.message.text.strip()
    if chave == '❌ Cancelar':
        await update.message.reply_text("❌ Saque cancelado.", reply_markup=teclado_principal())
        return ConversationHandler.END

    valor = context.user_data.get('saque_valor', 0)
    tipo = context.user_data.get('saque_tipo', 'aleatoria')
    context.user_data['saque_chave'] = chave

    keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("✅ Confirmar"), KeyboardButton("❌ Cancelar")]],
        resize_keyboard=True
    )

    tipo_label = {'telefone': 'Celular', 'email': 'E-mail', 'cpf': 'CPF', 'aleatoria': 'Aleatória'}.get(tipo, tipo)

    await update.message.reply_text(
        f"⚠️ *Confirme o saque:*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Valor: *R$ {valor:.2f}*\n"
        f"🔑 Tipo: {tipo_label}\n"
        f"📋 Chave: `{chave}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"_O saque é manual e será processado em até 24h._",
        parse_mode='Markdown',
        reply_markup=keyboard
    )
    return AGUARDANDO_CONFIRMACAO_SAQUE

async def confirmar_saque(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text.strip()
    if texto != '✅ Confirmar':
        await update.message.reply_text("❌ Saque cancelado.", reply_markup=teclado_principal())
        return ConversationHandler.END

    user = update.effective_user
    valor = context.user_data.get('saque_valor', 0)
    tipo = context.user_data.get('saque_tipo', 'aleatoria')
    chave = context.user_data.get('saque_chave', '')

    resultado = mp2_solicitar_saque(user.id, valor, chave, tipo)

    if resultado.get('success'):
        saque_id = resultado.get('saque_id', '?')
        await update.message.reply_text(
            f"✅ *Saque registrado com sucesso!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 Pedido: #{saque_id}\n"
            f"💰 Valor: R$ {valor:.2f}\n"
            f"📋 Chave: {chave}\n\n"
            f"⏱️ Prazo: até 24 horas úteis\n"
            f"_Você receberá uma notificação quando processado._",
            parse_mode='Markdown',
            reply_markup=teclado_principal()
        )
    else:
        await update.message.reply_text(
            f"❌ {resultado.get('error', 'Erro ao registrar saque.')}",
            reply_markup=teclado_principal()
        )

    return ConversationHandler.END

# ─── HISTÓRICO ────────────────────────────────────────────────────────────────
async def cmd_historico(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    mp2_get_ou_criar_usuario(user.id, user.username or '', user.full_name or '')
    historico = mp2_historico(user.id, limite=10)

    if not historico:
        await update.message.reply_text(
            "📊 Nenhuma transação encontrada ainda.",
            reply_markup=teclado_principal()
        )
        return

    linhas = []
    emoji_tipo = {'deposito': '📥', 'saque': '📤', 'comissao': '🎁'}
    emoji_status = {'confirmado': '✅', 'pendente': '⏳', 'cancelado': '❌', 'rejeitado': '🚫'}

    for tx in historico:
        tipo = tx.get('tipo', '')
        valor = float(tx.get('valor', 0))
        status = tx.get('status', '')
        data = tx.get('criado_em', '')
        if data:
            try:
                data = str(data)[:16].replace('T', ' ')
            except:
                pass
        linhas.append(
            f"{emoji_tipo.get(tipo, '💰')} {tipo.capitalize()} R$ {valor:.2f} "
            f"{emoji_status.get(status, '')} {data}"
        )

    texto = "📊 *Últimas transações:*\n━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(linhas)
    await update.message.reply_text(texto, parse_mode='Markdown', reply_markup=teclado_principal())

# ─── INDICAR (REFERRAL) ──────────────────────────────────────────────────────
async def cmd_indicar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot_username = (await context.bot.get_me()).username
    link = f"https://t.me/{bot_username}?start=ref_{user.id}"

    await update.message.reply_text(
        f"👥 *Programa de Indicação*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Compartilhe seu link e ganhe bônus!\n\n"
        f"🔗 Seu link:\n`{link}`\n\n"
        f"_Toque para copiar_",
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── AJUDA ────────────────────────────────────────────────────────────────────
async def cmd_ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dep_min = mp2_get_config('deposito_minimo', '5')
    dep_max = mp2_get_config('deposito_maximo', '10000')
    saque_min = mp2_get_config('saque_minimo', '20')

    await update.message.reply_text(
        f"ℹ️ *Ajuda — PayPixNex*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 *Depósito via Pix*\n"
        f"  Min: R$ {dep_min} | Max: R$ {dep_max}\n"
        f"  Crédito imediato após confirmação\n\n"
        f"📤 *Saque Manual*\n"
        f"  Min: R$ {saque_min}\n"
        f"  Prazo: até 24h úteis\n\n"
        f"👥 *Indicação*\n"
        f"  Ganhe bônus ao indicar amigos!\n\n"
        f"❓ Dúvidas? Contate o suporte.",
        parse_mode='Markdown',
        reply_markup=teclado_principal()
    )

# ─── CALLBACKS (InlineKeyboard) ──────────────────────────────────────────────
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith('copyfix_'):
        payment_id = data.replace('copyfix_', '')
        await query.edit_message_text(
            f"✅ Use o código acima para pagar!\n\n"
            f"_ID: {payment_id}_",
            parse_mode='Markdown'
        )

    elif data.startswith('checkpix_'):
        payment_id = data.replace('checkpix_', '')
        from mp2_api import mp2_verificar_pagamento
        status_info = mp2_verificar_pagamento(payment_id)
        status = status_info.get('status', 'pending')

        if status == 'approved':
            await query.edit_message_text(
                "✅ *Pagamento confirmado!*\n"
                "Seu saldo foi atualizado. Use /carteira para verificar.",
                parse_mode='Markdown'
            )
        elif status in ('rejected', 'cancelled'):
            await query.edit_message_text(
                f"❌ Pagamento {status}. Tente gerar um novo Pix.",
                parse_mode='Markdown'
            )
        else:
            await query.edit_message_text(
                "⏳ Pagamento ainda pendente.\n"
                "Aguarde alguns segundos após pagar e tente novamente.\n\n"
                "Se já pagou há mais de 5 minutos, contate o suporte.",
                parse_mode='Markdown'
            )

# ─── HANDLER TEXTO GENÉRICO ──────────────────────────────────────────────────
async def handler_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    texto = update.message.text.strip()
    mapa = {
        "💰 Depositar": cmd_depositar,
        "💼 Carteira":  cmd_carteira,
        "📤 Sacar":     cmd_sacar,
        "📊 Histórico": cmd_historico,
        "👥 Indicar":   cmd_indicar,
        "ℹ️ Ajuda":     cmd_ajuda,
    }
    handler = mapa.get(texto)
    if handler:
        await handler(update, context)
    else:
        await update.message.reply_text(
            "❓ Comando não reconhecido. Use o menu abaixo:",
            reply_markup=teclado_principal()
        )

# ─── CANCELAR GENÉRICO ───────────────────────────────────────────────────────
async def cmd_cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❌ Operação cancelada.",
        reply_markup=teclado_principal()
    )
    return ConversationHandler.END

# ─── BUILD APPLICATION ────────────────────────────────────────────────────────
def build_bot2_app():
    """Constrói e retorna a Application do Bot 2."""
    if not BOT2_TOKEN:
        logger.warning('⚠️ BOT2_TOKEN não configurado — Bot2 desativado')
        return None

    app = Application.builder().token(BOT2_TOKEN).build()

    # ConversationHandler — Depósito
    conv_deposito = ConversationHandler(
        entry_points=[
            CommandHandler('depositar', cmd_depositar),
            MessageHandler(filters.Regex('^💰 Depositar$'), cmd_depositar),
        ],
        states={
            AGUARDANDO_VALOR_DEP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receber_valor_dep)
            ],
        },
        fallbacks=[
            CommandHandler('cancelar', cmd_cancelar),
            MessageHandler(filters.Regex('^❌ Cancelar$'), cmd_cancelar),
        ],
    )

    # ConversationHandler — Saque
    conv_saque = ConversationHandler(
        entry_points=[
            CommandHandler('sacar', cmd_sacar),
            MessageHandler(filters.Regex('^📤 Sacar$'), cmd_sacar),
        ],
        states={
            AGUARDANDO_VALOR_SAQUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receber_valor_saque)
            ],
            AGUARDANDO_TIPO_CHAVE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receber_tipo_chave)
            ],
            AGUARDANDO_CHAVE_PIX: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receber_chave_pix)
            ],
            AGUARDANDO_CONFIRMACAO_SAQUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, confirmar_saque)
            ],
        },
        fallbacks=[
            CommandHandler('cancelar', cmd_cancelar),
            MessageHandler(filters.Regex('^❌ Cancelar$'), cmd_cancelar),
        ],
    )

    # Registrar handlers
    app.add_handler(CommandHandler('start',    cmd_start))
    app.add_handler(CommandHandler('carteira', cmd_carteira))
    app.add_handler(CommandHandler('historico', cmd_historico))
    app.add_handler(CommandHandler('indicar',  cmd_indicar))
    app.add_handler(CommandHandler('ajuda',    cmd_ajuda))
    app.add_handler(conv_deposito)
    app.add_handler(conv_saque)
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handler_texto))

    logger.info('✅ Bot2 (@paypix_nexbot) configurado!')
    return app

# ─── ENTRYPOINT STANDALONE (para testes) ─────────────────────────────────────
async def iniciar_bot2_polling():
    """Inicia o Bot2 em modo polling (para rodar standalone ou dentro do loop do aiohttp)."""
    init_mp2_db()
    app = build_bot2_app()
    if not app:
        return
    logger.info('🤖 [Bot2] Iniciando polling...')
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    logger.info('✅ [Bot2] Polling ativo!')
    # Manter rodando
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(iniciar_bot2_polling())
