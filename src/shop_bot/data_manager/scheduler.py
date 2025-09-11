import asyncio
import logging

from datetime import datetime, timedelta

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import Bot

from shop_bot.bot_controller import BotController
from shop_bot.data_manager import database
from shop_bot.modules import xui_api
from shop_bot.bot import keyboards

CHECK_INTERVAL_SECONDS = 300
NOTIFY_BEFORE_HOURS = {72, 48, 24, 1}
notified_users = {}

logger = logging.getLogger(__name__)

def format_time_left(hours: int) -> str:
    if hours >= 24:
        days = hours // 24
        if days % 10 == 1 and days % 100 != 11:
            return f"{days} день"
        elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
            return f"{days} дня"
        else:
            return f"{days} дней"
    else:
        if hours % 10 == 1 and hours % 100 != 11:
            return f"{hours} час"
        elif 2 <= hours % 10 <= 4 and (hours % 100 < 10 or hours % 100 >= 20):
            return f"{hours} часа"
        else:
            return f"{hours} часов"

async def send_subscription_notification(bot: Bot, user_id: int, key_id: int, time_left_hours: int, expiry_date: datetime):
    try:
        time_text = format_time_left(time_left_hours)
        expiry_str = expiry_date.strftime('%d.%m.%Y в %H:%M')
        
        message = (
            f"⚠️ **Внимание!** ⚠️\n\n"
            f"Срок действия вашей подписки истекает через **{time_text}**.\n"
            f"Дата окончания: **{expiry_str}**\n\n"
            f"Продлите подписку, чтобы не остаться без доступа к VPN!"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔑 Мои ключи", callback_data="manage_keys")
        builder.button(text="➕ Продлить ключ", callback_data=f"extend_key_{key_id}")
        builder.adjust(2)
        
        await bot.send_message(chat_id=user_id, text=message, reply_markup=builder.as_markup(), parse_mode='Markdown')
        logger.info(f"Sent subscription notification to user {user_id} for key {key_id} ({time_left_hours} hours left).")
        
    except Exception as e:
        logger.error(f"Error sending subscription notification to user {user_id}: {e}")

def _cleanup_notified_users(all_db_keys: list[dict]):
    if not notified_users:
        return

    logger.info("Scheduler: Cleaning up the notification cache...")
    
    active_key_ids = {key['key_id'] for key in all_db_keys}
    
    users_to_check = list(notified_users.keys())
    
    cleaned_users = 0
    cleaned_keys = 0

    for user_id in users_to_check:
        keys_to_check = list(notified_users[user_id].keys())
        for key_id in keys_to_check:
            if key_id not in active_key_ids:
                del notified_users[user_id][key_id]
                cleaned_keys += 1
        
        if not notified_users[user_id]:
            del notified_users[user_id]
            cleaned_users += 1
    
    if cleaned_users > 0 or cleaned_keys > 0:
        logger.info(f"Scheduler: Cleanup complete. Removed {cleaned_users} user entries and {cleaned_keys} key entries from the cache.")

async def check_expiring_subscriptions(bot: Bot):
    logger.info("Scheduler: Checking for expiring subscriptions...")
    current_time = datetime.now()
    all_keys = database.get_all_keys()
    
    _cleanup_notified_users(all_keys)
    
    for key in all_keys:
        try:
            expiry_date = datetime.fromisoformat(key['expiry_date'])
            time_left = expiry_date - current_time

            if time_left.total_seconds() < 0:
                continue

            total_hours_left = int(time_left.total_seconds() / 3600)
            user_id = key['user_id']
            key_id = key['key_id']

            for hours_mark in NOTIFY_BEFORE_HOURS:
                if hours_mark - 1 < total_hours_left <= hours_mark:
                    notified_users.setdefault(user_id, {}).setdefault(key_id, set())
                    
                    if hours_mark not in notified_users[user_id][key_id]:
                        await send_subscription_notification(bot, user_id, key_id, hours_mark, expiry_date)
                        notified_users[user_id][key_id].add(hours_mark)
                    break 
                    
        except Exception as e:
            logger.error(f"Error processing expiry for key {key.get('key_id')}: {e}")

async def sync_keys_with_panels(bot=None):
    logger.info("Scheduler: Starting sync with XUI panels...")
    total_affected_records = 0
    
    all_hosts = database.get_all_hosts()
    if not all_hosts:
        logger.info("Scheduler: No hosts configured in the database. Sync skipped.")
        return

    for host in all_hosts:
        host_name = host['host_name']
        logger.info(f"Scheduler: Processing host: '{host_name}'")
        
        try:
            api, inbound = xui_api.login_to_host(
                host_url=host['host_url'],
                username=host['host_username'],
                password=host['host_pass'],
                inbound_id=host['host_inbound_id']
            )

            if not api or not inbound:
                logger.error(f"Scheduler: Could not log in to host '{host_name}'. Skipping this host.")
                continue
            
            full_inbound_details = api.inbound.get_by_id(inbound.id)
            clients_on_server = {client.email: client for client in (full_inbound_details.settings.clients or [])}
            logger.info(f"Scheduler: Found {len(clients_on_server)} clients on the '{host_name}' panel.")

            keys_in_db = database.get_keys_for_host(host_name)
            
            for db_key in keys_in_db:
                key_email = db_key['key_email']
                expiry_date = datetime.fromisoformat(db_key['expiry_date'])
                now = datetime.now()
                if expiry_date < now - timedelta(days=5):
                    logger.info(f"Scheduler: Key '{key_email}' expired more than 5 days ago. Deleting from panel and DB.")
                    try:
                        await xui_api.delete_client_on_host(host_name, key_email)
                    except Exception as e:
                        logger.error(f"Scheduler: Failed to delete client '{key_email}' from panel: {e}")
                    database.delete_key_by_email(key_email)
                    total_affected_records += 1
                    continue

                server_client = clients_on_server.pop(key_email, None)

                if server_client:
                    reset_days = server_client.reset if server_client.reset is not None else 0
                    server_expiry_ms = server_client.expiry_time + reset_days * 24 * 3600 * 1000
                    local_expiry_dt = expiry_date
                    local_expiry_ms = int(local_expiry_dt.timestamp() * 1000)

                    if abs(server_expiry_ms - local_expiry_ms) > 1000:
                        database.update_key_status_from_server(key_email, server_client)
                        total_affected_records += 1
                        logger.info(f"Scheduler: Synced (updated) key '{key_email}' for host '{host_name}'.")
                else:
                    logger.warning(f"Scheduler: Key '{key_email}' for host '{host_name}' not found on server. Deleting from local DB.")
                    database.update_key_status_from_server(key_email, None)
                    total_affected_records += 1
                # After syncing keys, attempt to enforce per-key max_connections limits (best-effort)
                try:
                    enforce_flag = database.get_setting('enforce_max_connections')
                    if enforce_flag == 'true':
                        await enforce_connection_limits_for_host(api, inbound, full_inbound_details, keys_in_db, host_name, bot)
                    else:
                        logger.debug(f"Scheduler: Enforcement disabled in settings for host '{host_name}'.")
                except Exception as e:
                    logger.error(f"Scheduler: Error during enforcement for host '{host_name}': {e}")
            if clients_on_server:
                for orphan_email in clients_on_server.keys():
                    logger.warning(f"Scheduler: Found orphan client '{orphan_email}' on host '{host_name}' that is not tracked by the bot.")

        except Exception as e:
            logger.error(f"Scheduler: An unexpected error occurred while processing host '{host_name}': {e}", exc_info=True)
            
    logger.info(f"Scheduler: Sync with XUI panels finished. Total records affected: {total_affected_records}.")

async def periodic_subscription_check(bot_controller: BotController):
    logger.info("Scheduler has been started.")
    await asyncio.sleep(10)

    while True:
        try:
            bot = None
            try:
                bot = bot_controller.get_bot_instance()
            except Exception:
                bot = None

            await sync_keys_with_panels(bot=bot)

            if bot_controller.get_status().get("is_running"):
                bot = bot_controller.get_bot_instance()
                if bot:
                    await check_expiring_subscriptions(bot)
                else:
                    logger.warning("Scheduler: Bot is marked as running, but instance is not available.")
            else:
                logger.info("Scheduler: Bot is stopped, skipping user notifications.")

        except Exception as e:
            logger.error(f"Scheduler: An unhandled error occurred in the main loop: {e}", exc_info=True)
            
        logger.info(f"Scheduler: Cycle finished. Next check in {CHECK_INTERVAL_SECONDS} seconds.")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


async def enforce_connection_limits_for_host(api, inbound, full_inbound_details, keys_in_db, host_name: str, bot = None):
    """Best-effort enforcement: try to read per-client active connection counts and disable clients exceeding DB limits.
    Many XUI panels may not expose live connection counts for each client. This function will attempt several heuristics:
    - check commonly named attributes on client objects for active connection counts (active, conn_count, current_connections)
    - if a client has max_connections set in DB and we can read a current connections metric, disable the client via api.client.update if exceeded
    - if we cannot read connection counts, log and skip enforcement
    """
    logger.info(f"Enforcement: Checking connection limits for host '{host_name}'...")

    # Build map of email->db record for quick lookup
    db_map = {k['key_email']: k for k in keys_in_db}

    clients = full_inbound_details.settings.clients or []
    any_action = False

    for client in clients:
        try:
            email = getattr(client, 'email', None)
            if not email or email not in db_map:
                continue

            db_record = db_map[email]
            max_conn = db_record.get('max_connections')
            if not max_conn:
                # fallback to global default if configured (>0)
                try:
                    global_default = database.get_setting('default_max_connections')
                    if global_default and global_default.isdigit() and int(global_default) > 0:
                        max_conn = int(global_default)
                    else:
                        continue
                except Exception:
                    continue

            # try to discover current active connections on the client object
            current = None
            for attr in ('active', 'conn_count', 'current_connections', 'connections', 'online_count'):
                if hasattr(client, attr):
                    try:
                        current = int(getattr(client, attr))
                        break
                    except Exception:
                        continue

            if current is None:
                logger.info(f"Enforcement: Could not determine current connections for client {email} on host {host_name}; skipping.")
                continue

            if current > int(max_conn):
                logger.warning(f"Enforcement: Client {email} has {current} connections > max {max_conn}; attempting to disable client.")
                # best-effort: try to set enable=False on client and update inbound
                try:
                    client.enable = False
                    api.inbound.update(inbound.id, full_inbound_details)
                    logger.info(f"Enforcement: Disabled client {email} on host {host_name}.")
                    any_action = True

                    # update DB: nothing to change except keep record

                    # Notify admin and user if bot is available
                    try:
                        admin_id = int(database.get_setting('admin_telegram_id') or 0)
                    except Exception:
                        admin_id = 0

                    user_id = int(db_record.get('user_id') or 0)

                    notify_text_user = (
                        f"⚠️ Ваш ключ ({email}) был временно отключён администратором из-за превышения допустимого числа одновременных подключений ({current} > {max_conn}).\n"
                        f"Обратитесь в поддержку для решения вопроса."
                    )

                    notify_text_admin = (
                        f"Enforcement: Client {email} on host {host_name} was disabled automatically: {current} active connections > max {max_conn} (user_id={user_id})."
                    )

                    if bot:
                        try:
                            # send user notification
                            if user_id:
                                await bot.send_message(chat_id=user_id, text=notify_text_user)
                            # send admin notification
                            if admin_id:
                                await bot.send_message(chat_id=admin_id, text=notify_text_admin)
                        except Exception as e:
                            logger.error(f"Enforcement: Failed to send notifications for client {email}: {e}")

                except Exception as e:
                    logger.error(f"Enforcement: Failed to disable client {email} on host {host_name}: {e}")

        except Exception as e:
            logger.error(f"Enforcement: Error processing client in host {host_name}: {e}")

    if not any_action:
        logger.info(f"Enforcement: No enforcement actions taken for host '{host_name}'.")