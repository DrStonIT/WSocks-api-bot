import asyncpg
import logging
from datetime import datetime, timezone, timedelta


async def init_pool(dsn):
    """Инициализация пула соединений"""
    return await asyncpg.create_pool(dsn)

#-------------------------------------------------------------------------------------------------------------------------------------------
#VPN subs system

async def add_subscription_to_db(tg_id, email, panel, expiry_date, pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (tg_id, email, panel, expiry_date, warn, ends) VALUES ($1, $2, $3, $4, 0, 0)",
            tg_id, email, panel, expiry_date
        )
        logging.info(f"Подписка добавлена: {email}")


async def update_subscriptions_on_db(tg_id, email, panel, expiry_date,  pool):
    async with pool.acquire() as conn:
        # Проверяем, существует ли подписка с указанным email
        existing = await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)

        if existing:
            # Если подписка существует, обновляем её
            await conn.execute(
                "UPDATE users SET expiry_date = $1, warn = 0, ends = 0 WHERE email = $2",
                expiry_date, email
            )
            logging.info(f"Подписка обновлена: {email}")
        else:
            # Если подписка отсутствует, создаем новую
            tg_id = tg_id or "unknown"  # Используем переданный tg_id или "unknown", если не указан
            await conn.execute(
                "INSERT INTO users (tg_id, email, panel, expiry_date, warn, ends) VALUES ($1, $2, $3, $4, 0, 0)",
                tg_id, email, panel, expiry_date
            )
            logging.info(f"Новая подписка создана: {email}")

async def add_payment_to_db(telegram_id, label, operation_type, payment_time, amount, email, pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO payments (telegram_id, label, operation_type, payment_time, amount, email) VALUES ($1, $2, $3, $4, $5, $6)",
            telegram_id, label, operation_type, payment_time, amount, email
        )
        logging.info(f"Платёж добавлен: {email}")

#-------------------------------------------------------------------------------------------------------------------------------------------
#Trial system

async def get_trial_status(tg_id, pool):
    """Получение статуса пробного периода"""
    async with pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT status FROM trials WHERE tg_id = $1", tg_id
        )
        return 1 if result == 1 else 0

async def create_trial_user(tg_id, pool):
    """Создание пробного пользователя"""
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO trials (tg_id, status) VALUES ($1, 1)", tg_id
        )
        logging.info(f"Пробный пользователь создан: tg_id={tg_id}")

#-------------------------------------------------------------------------------------------------------------------------------------------
#Referal system

async def get_referrals(tg_id, pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            '''
            SELECT referee_id, bonus_applied, bonus_date
            FROM referrals
            WHERE referrer_id = $1
            ''',
            tg_id
        )
        return [
            {
                'referee_id': row['referee_id'],
                'bonus_applied': row['bonus_applied'],
                'bonus_date': row['bonus_date']
            }
            for row in rows
        ]

async def apply_referral_bonus_db(referrer_id, referee_id, pool):
    """Применение реферального бонуса"""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE referrals SET bonus_applied = 1, bonus_date = $1 WHERE referrer_id = $2 AND referee_id = $3",
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), referrer_id, referee_id
        )
        logging.info(f"Бонус применён: referrer_id={referrer_id}, referee_id={referee_id}")

#-------------------------------------------------------------------------------------------------------------------------------------------
#Products system

async def add_product_to_db(tg_id, product, login, days, pool):
    async with pool.acquire() as conn:
        # Проверяем, существует ли уже запись с таким email
        existing = await conn.fetchrow("SELECT * FROM products WHERE login = $1 AND product = $2", login, product)

        now = datetime.now(timezone.utc)

        if existing:
            current_expiry_str = existing["expiry_date"]
            current_expiry = datetime.strptime(current_expiry_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

            if current_expiry < now:
                new_expiry = now + timedelta(days=days)
            else:
                new_expiry = current_expiry + timedelta(days=days)

            new_expiry_str = new_expiry.strftime("%Y-%m-%d %H:%M:%S")
            await conn.execute(
                "UPDATE products SET expiry_date = $1 WHERE login = $2",
                new_expiry_str, login
            )
            logging.info(f"Подписка обновлена: {tg_id}")
        else:
            expiry_date = (now + timedelta(days)).strftime("%Y-%m-%d %H:%M:%S")
            await conn.execute(
                """
                INSERT INTO products (tg_id, product, login, expiry_date)
                VALUES ($1, $2, $3, $4)
                """,
                tg_id, product, login, expiry_date
            )
            logging.info(f"Подписка добавлена: {tg_id}")