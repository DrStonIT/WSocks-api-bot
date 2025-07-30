import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
import json
import uuid
import logging
import hmac
import hashlib
import urllib.parse
from fastapi.middleware.cors import CORSMiddleware
from py3xui import Client
import random
import string
import asyncpg
import config as cfg
from contextlib import asynccontextmanager
from xui_utils import get_best_panel, get_api_by_name, get_active_subscriptions, extend_subscription, create_sub_panel_subscriptions, \
    extend_sub_panel_subscriptions
from database import add_payment_to_db, add_subscription_to_db, update_subscriptions_on_db, create_trial_user, \
    get_trial_status, get_referrals, apply_referral_bonus_db, add_product_to_db
from yookassa import Configuration, Payment




pool = None

Configuration.account_id = cfg.YOOKASSA_SHOP_ID
Configuration.secret_key = cfg.YOOKASSA_SECRET_KEY

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройка CORS



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    global pool
    pool = await asyncpg.create_pool(
        cfg.DSN,
        min_size=2,
        max_size=5,
        max_inactive_connection_lifetime=300
    )
    logger.info("Database pool initialized")

    try:
        yield  # Application runs here
    finally:
        # Shutdown logic
        await pool.close()
        logger.info("Database pool closed")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://wsocksminiapp.netlify.app", "https://telegram.org"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def generate_sub(length=16):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def verify_init_data(init_data: str) -> dict:
    try:
        if not init_data:
            raise HTTPException(status_code=422, detail="init_data is empty")
        parsed_data = dict(urllib.parse.parse_qsl(init_data))
        logger.info(f"Parsed init_data: {parsed_data}")
        received_hash = parsed_data.pop('hash', None)
        if not received_hash:
            raise HTTPException(status_code=422, detail="Hash not found")
        data_check_string = '\n'.join(f'{k}={v}' for k, v in sorted(parsed_data.items()))
        secret_key = hmac.new("WebAppData".encode(), cfg.MAIN_API_TOKEN.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if computed_hash != received_hash:
            raise HTTPException(status_code=401, detail="Invalid auth data")
        user_data = urllib.parse.parse_qs(init_data).get('user', [''])[0]
        if not user_data:
            raise HTTPException(status_code=422, detail="User data not found")
        try:
            return {'user': json.loads(user_data)}
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            raise HTTPException(status_code=422, detail=f"Invalid user data format: {str(e)}")
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error verifying initData: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing initData: {str(e)}")


@app.get("/")
async def root():
    logger.info("Root endpoint accessed")
    return {"status": "OK"}


class AuthData(BaseModel):
    init_data: str


class BuySubscriptionData(BaseModel):
    tg_id: int
    days: int


class ExtendSubscriptionData(BaseModel):
    tg_id: int
    days: int
    email: str


class TrialSubscriptionData(BaseModel):
    tg_id: int


class ApplyReferralBonusData(BaseModel):
    tg_id: int
    referee_id: int
    email: str | None = None

class CheckPaymentData(BaseModel):
    payment_id: str

class BuyProductData(BaseModel):
    tg_id: int
    product: str
    login: str
    password: str
    days: int
    amount: int

@app.post("/api/auth")
async def auth(data: AuthData):
    logger.info(f"Received init_data: {data.init_data}")
    try:
        user_data = verify_init_data(data.init_data)
        tg_id = user_data['user']['id']
        first_name = user_data['user'].get('first_name', '')
        logger.info(f"Authenticated user: {tg_id}")
        return {"user": {"telegram_id": tg_id, "first_name": first_name}}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Auth error: {e}")
        raise HTTPException(status_code=500, detail=f"Auth error: {str(e)}")


@app.get("/api/subscriptions")
async def get_subscriptions(tg_id: int):
    logger.info(f"Fetching subscriptions for tg_id: {tg_id}")
    try:
        subscriptions = get_active_subscriptions(tg_id)
        formatted_subscriptions = [
            {
                "email": sub['email'],
                "panel": sub['panel'],
                "expiry_date": sub['expiry_date'].strftime("%Y-%m-%d %H:%M:%S"),
                "is_expired": sub['is_expired'],
                "sub_url": sub['sub_link'],
                "redirect_url": f'{cfg.BASE_REDIRECT_URL}/?key={urllib.parse.quote(sub['sub_link'], safe="")}'
            }
            for sub in subscriptions
        ]
        logger.info(f"Subscriptions fetched: {formatted_subscriptions}")
        return {"subscriptions": formatted_subscriptions}
    except Exception as e:
        logger.error(f"Error fetching subscriptions: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching subscriptions: {str(e)}")


@app.get("/api/referrals")
async def get_referrals_endpoint(tg_id: int):
    logger.info(f"Fetching referrals for tg_id: {tg_id}")
    try:
        referrals = await get_referrals(str(tg_id), pool)
        formatted_referrals = [
            {
                "referee_id": ref['referee_id'],
                "bonus_applied": ref['bonus_applied'],
                "bonus_date": ref['bonus_date'] if ref['bonus_date'] else None
            }
            for ref in referrals
        ]
        logger.info(f"Referrals fetched: {formatted_referrals}")
        return {"referrals": formatted_referrals}
    except Exception as e:
        logger.error(f"Error fetching referrals: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching referrals: {str(e)}")

@app.post("/api/buy-product")
async def buy_product(data: BuyProductData):
    try:
        amount = data.amount

        payment = Payment.create({
            "amount": {"value": str(amount), "currency": "RUB"},
            "confirmation": {"type": "redirect", "return_url": "https://your-app.com/payment"},
            "capture": True,
            "description": f"Покупка товара {data.product}",
            "metadata": {
                "tg_id": data.tg_id,
                "product": data.product,
                "login": data.login,
                "password": data.password,
                "days": data.days,
                "is_product": True,
                "amount": amount
            }
        })

        return {
            "payment_url": payment.confirmation.confirmation_url,
            "payment_id": payment.id
        }
    except Exception as e:
        logger.error(f"Ошибка при создании оплаты товара: {e}")
        raise HTTPException(status_code=500, detail="Ошибка при создании оплаты товара")

@app.post("/api/buy-subscription")
async def buy_subscription(data: BuySubscriptionData):
    logger.info(f"Creating subscription for tg_id: {data.tg_id}, days: {data.days}")
    try:
        if data.days not in [7, 30, 90, 180, 360]:
            raise HTTPException(status_code=400, detail="Invalid subscription period")
        prices = {7: 0, 30: 89, 90: 249, 180: 449, 360: 849}
        amount = prices[data.days]
        email = f"DE-FRA-USER-{data.tg_id}-{uuid.uuid4().hex[:6]}"
        # Вычисляем дату окончания
        expiry_date = datetime.now(timezone.utc) + timedelta(days=data.days)

        payment = Payment.create({
            "amount": {"value": str(amount), "currency": "RUB"},
            "confirmation": {"type": "redirect", "return_url": "https://your-app.com/payment"},
            "capture": True,
            "description": f"Покупка подписки на {data.days} дней",
            "metadata": {"tg_id": data.tg_id, "days": data.days, "email": email, "is_extension": False}
        })

        payment_id = payment.id
        logger.info(f"Payment created for tg_id: {data.tg_id}, payment_id: {payment_id}")
        return {
            "email": email,
            "payment_url": payment.confirmation.confirmation_url,
            "payment_id": payment_id,
            "expiry_date": expiry_date.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logger.error(f"Error creating subscription: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating subscription: {str(e)}")

@app.post("/api/extend-subscription")
async def extend_subscription_endpoint(data: ExtendSubscriptionData):
    logger.info(f"Extending subscription for tg_id: {data.tg_id}, email: {data.email}, days: {data.days}")
    try:
        if data.days not in [7, 30, 90, 180, 360]:
            raise HTTPException(status_code=400, detail="Invalid subscription period")
        subscriptions = get_active_subscriptions(data.tg_id)
        selected_sub = next((sub for sub in subscriptions if sub['email'] == data.email), None)
        if not selected_sub:
            raise HTTPException(status_code=404, detail="Subscription not found")
        if selected_sub['email'].startswith("DE-FRA-TRIAL-"):
            raise HTTPException(status_code=400, detail="Trial subscriptions cannot be extended")

        amount = {7: 0, 30: 89, 90: 249, 180: 449, 360: 849}[data.days]
        # Вычисляем дату окончания
        start_date = datetime.now(timezone.utc) if selected_sub['is_expired'] else selected_sub['expiry_date']
        expiry_date = start_date + timedelta(days=data.days)

        payment = Payment.create({
            "amount": {"value": str(amount), "currency": "RUB"},
            "confirmation": {"type": "redirect", "return_url": "https://your-app.com/payment"},
            "capture": True,
            "description": f"Продление подписки на {data.days} дней",
            "metadata": {"tg_id": data.tg_id, "days": data.days, "email": data.email, "is_extension": True}
        })

        payment_id = payment.id
        logger.info(f"Payment created for extending subscription: {data.email}, payment_id: {payment_id}")
        return {
            "email": data.email,
            "payment_url": payment.confirmation.confirmation_url,
            "payment_id": payment_id,
            "expiry_date": expiry_date.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logger.error(f"Error extending subscription: {e}")
        raise HTTPException(status_code=500, detail=f"Error extending subscription: {str(e)}")

@app.post("/api/check-payment-status")
async def check_payment_status(data: CheckPaymentData):
    try:
        payment = Payment.find_one(data.payment_id)
        logger.info(f"Payment status for payment_id: {data.payment_id}: {payment.status}")
        logger.info(f"Full payment object: {payment.__dict__}")
        logger.info(f"Full payment object: {payment.metadata['is_extension']}")
        if payment.status == 'succeeded':
            metadata = payment.metadata
            tg_id = int(metadata['tg_id'])
            days = int(metadata['days'])
            email = metadata['email']
            is_extension = metadata['is_extension']

            subscriptions = get_active_subscriptions(tg_id)
            selected_sub = next((sub for sub in subscriptions if sub['email'] == email), None)

            expiry_time = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

            logger.info(f"Selected_sub: {selected_sub}")
            logger.info(f"is_extension: {is_extension}")

            if is_extension and selected_sub:
                # Продление подписки
                api = get_api_by_name(selected_sub['panel'])
                client_found = False
                inbounds = api.inbound.get_list()
                for inbound in inbounds:
                    for client in inbound.settings.clients:
                        if client.email == email and client.tg_id == tg_id:
                            extend_subscription(client.email, client.id, days, tg_id, client.sub_id, api)
                            extend_sub_panel_subscriptions(client.email, days, tg_id, client.sub_id)
                            new_expiry = (datetime.now(timezone.utc) if selected_sub['is_expired'] else selected_sub['expiry_date']) + timedelta(days=days)
                            expiry_time = new_expiry.strftime("%Y-%m-%d %H:%M:%S")
                            await update_subscriptions_on_db(str(tg_id), email, selected_sub['panel'], expiry_time, pool)
                            await add_payment_to_db(str(tg_id), payment.id, 'Продление', expiry_time, payment.amount.value, email, pool)
                            client_found = True
                            break
                    if client_found:
                        break
                if not client_found:
                    raise HTTPException(status_code=404, detail="Client not found")
            else:
                # Новая подписка
                if selected_sub:
                    raise HTTPException(status_code=400, detail="Subscription with this email already exists")
                current_panel = get_best_panel()
                if not current_panel:
                    raise HTTPException(status_code=500, detail="No available panels")
                subscription_id = generate_sub(16)
                expiry = int(datetime.strptime(expiry_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
                new_client = Client(
                    id=str(uuid.uuid4()),
                    enable=True,
                    tg_id=int(tg_id),
                    expiry_time=expiry,
                    flow="xtls-rprx-vision",
                    email=email,
                    sub_id=subscription_id,
                    limit_ip=5
                )
                api = get_api_by_name(current_panel['name'])
                api.client.add(1, [new_client])
                create_sub_panel_subscriptions(email, int(tg_id), subscription_id, expiry)

                await add_subscription_to_db(str(tg_id), email, current_panel['name'], expiry_time, pool)
                await add_payment_to_db(str(tg_id), payment.id, 'Покупка', expiry_time, payment.amount.value, email, pool)

            return {
                "status": payment.status,
                "days": days,
                "expiry_date": expiry_time
            }
        else:
            return {"status": payment.status}
    except Exception as e:
        logger.error(f"Error checking payment status: {e}")
        raise HTTPException(status_code=500, detail=f"Error checking payment status: {str(e)}")

@app.post("/api/check-product-payment")
async def check_product_payment(data: CheckPaymentData):
    try:
        payment = Payment.find_one(data.payment_id)
        logger.info(f"[Product] Payment status for {data.payment_id}: {payment.status}")

        if payment.status != 'succeeded':
            return {"status": payment.status}

        metadata = getattr(payment, "metadata", None)
        if not metadata or not metadata.get("is_product"):
            logger.error(f"[Product] Metadata missing or invalid for payment {payment.id}")
            raise HTTPException(status_code=500, detail="Invalid or missing metadata")

        await add_product_to_db(
            tg_id=metadata['tg_id'],
            product=metadata['product'],
            login=metadata['login'],
            days=int(metadata['days']),
            pool=pool
        )

        message = (
            f"✅ Оплачен товар:\n"
            f"Telegram ID: {metadata['tg_id']}\n"
            f"Товар: {metadata['product']}\n"
            f"Логин: {metadata['login']}\n"
            f"Пароль: {metadata['password']}"
        )

        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{cfg.ORDER_BOT_TOKEN}/sendMessage",
                json={"chat_id": cfg.ADMIN_TOKEN_1, "text": message},
            )
            await client.post(
                f"https://api.telegram.org/bot{cfg.ORDER_BOT_TOKEN}/sendMessage",
                json={"chat_id": cfg.ADMIN_TOKEN_2, "text": message},
            )



        await add_payment_to_db(
            str(metadata['tg_id']),
            payment.id,
            metadata['product'],
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            payment.amount.value,
            metadata['login'],
            pool
        )

        return {
            "status": "succeeded",
            "product": metadata['product']
        }

    except Exception as e:
        logger.error(f"[Product] Error checking payment: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка при проверке оплаты товара")

@app.post("/api/cancel-payment")
async def cancel_payment(data: CheckPaymentData):
    try:
        payment = Payment.find_one(data.payment_id)
        logger.info(f"Cancelling payment for payment_id: {data.payment_id}, current status: {payment.status}")
        if payment.status == 'pending':
            Payment.cancel(data.payment_id)
            logger.info(f"Payment {data.payment_id} cancelled successfully")
            return {"status": "cancelled"}
        else:
            logger.warning(f"Cannot cancel payment {data.payment_id}, status: {payment.status}")
            return {"status": payment.status}
    except Exception as e:
        logger.error(f"Error cancelling payment: {e}")
        raise HTTPException(status_code=500, detail=f"Error cancelling payment: {str(e)}")


@app.post("/api/activate-trial")
async def activate_trial(data: TrialSubscriptionData):
    logger.info(f"Activating trial subscription for tg_id: {data.tg_id}")
    try:
        trial_status = await get_trial_status(str(data.tg_id), pool)
        if trial_status == 1:
            raise HTTPException(status_code=400, detail="Вы уже активировали пробную подписку")
        email = f"DE-FRA-TRIAL-{data.tg_id}-{uuid.uuid4().hex[:6]}"
        current_panel = get_best_panel()
        if not current_panel:
            raise HTTPException(status_code=500, detail="No available panels")
        subscription_id = generate_sub(16)
        expiry_time = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
        expiry = int(datetime.strptime(expiry_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        new_client = Client(
            id=str(uuid.uuid4()),
            enable=True,
            tg_id=int(data.tg_id),
            expiry_time=expiry,
            flow="xtls-rprx-vision",
            email=email,
            sub_id=subscription_id,
            limit_ip=5
        )
        api = get_api_by_name(current_panel['name'])
        api.client.add(1, [new_client])
        create_sub_panel_subscriptions(email, int(data.tg_id), subscription_id, expiry)
        await add_subscription_to_db(str(data.tg_id), email, current_panel['name'], expiry_time, pool)
        await create_trial_user(str(data.tg_id), pool)
        subscription_key = current_panel["create_key"](new_client)
        logger.info(f"Trial subscription created for tg_id: {data.tg_id}, email: {email}")
        return {
            "email": email,
            "panel": current_panel['name'],
            "key": subscription_key,
            "expiry_date": expiry_time,
            "days": 3
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error creating trial subscription: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating trial subscription: {str(e)}")

@app.post("/api/submit-order")
async def submit_order(order: dict):
    if not order.get('login') or not order.get('password'):
        raise HTTPException(status_code=400, detail="Login and password cannot be empty")

    message = (
        f"Новый заказ:\n"
        f"Telegram ID: {order['telegram_id']}\n"
        f"Товар: {order['product']}\n"
        f"Логин: {order['login']}\n"
        f"Пароль: {order['password']}"
    )

    product = order['product'].split(" ")[0]

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"https://api.telegram.org/bot{cfg.ORDER_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": cfg.ADMIN_TOKEN_1,
                    "text": message,
                },
            )

            await add_product_to_db(str(order['telegram_id']), product, order['login'], order['days'], pool)

            if response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to send message to Telegram")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error sending message: {str(e)}")

    return {"status": "Order submitted successfully"}


@app.post("/api/apply-referral-bonus")
async def apply_referral_bonus(data: ApplyReferralBonusData):
    logger.info(f"Applying referral bonus for tg_id: {data.tg_id}, referee_id: {data.referee_id}, email: {data.email}")
    try:
        referrals = await get_referrals(str(data.tg_id), pool)
        logger.info(f"Referrals for tg_id {data.tg_id}: {referrals}")
        referral = next((ref for ref in referrals if ref['referee_id'] == str(data.referee_id)), None)
        if not referral:
            logger.error(f"Referral not found for tg_id: {data.tg_id}, referee_id: {data.referee_id}")
            raise HTTPException(status_code=404, detail="Referral not found")
        if referral['bonus_applied']:
            raise HTTPException(status_code=400, detail="Bonus already applied")

        subscriptions = get_active_subscriptions(data.tg_id)
        non_trial_subs = [sub for sub in subscriptions if not sub['email'].startswith("DE-FRA-TRIAL-")]
        logger.info(f"Non-trial subscriptions: {non_trial_subs}")

        if len(non_trial_subs) == 0:
            # Условие 1: Создать новую подписку на 7 дней
            email = f"DE-FRA-USER-{data.tg_id}-{uuid.uuid4().hex[:6]}"
            current_panel = get_best_panel()
            if not current_panel:
                raise HTTPException(status_code=500, detail="No available panels")
            subscription_id = generate_sub(16)
            expiry_time = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            expiry = int(datetime.strptime(expiry_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
            new_client = Client(
                id=str(uuid.uuid4()),
                enable=True,
                tg_id=data.tg_id,
                expiry_time=expiry,
                flow="xtls-rprx-vision",
                email=email,
                sub_id=subscription_id,
                limit_ip=5
            )
            api = get_api_by_name(current_panel['name'])
            api.client.add(1, [new_client])
            create_sub_panel_subscriptions(email, int(data.tg_id), subscription_id, expiry)
            await add_subscription_to_db(str(data.tg_id), email, current_panel['name'], expiry_time, pool)
            await add_payment_to_db(str(data.tg_id), "REFERRAL_BONUS", 'Реферальный бонус', expiry_time, 0, email, pool)
            subscription_key = current_panel["create_key"](new_client)
            await apply_referral_bonus_db(str(data.tg_id), str(data.referee_id), pool)
            logger.info(f"Referral bonus created subscription for tg_id: {data.tg_id}, email: {email}")
            return {
                "email": email,
                "panel": current_panel['name'],
                "key": subscription_key,
                "expiry_date": expiry_time,
                "days": 7
            }
        elif len(non_trial_subs) == 1:
            # Условие 2: Автоматически продлить единственную подписку на 7 дней
            selected_sub = non_trial_subs[0]
            selected_email = selected_sub['email']
            api = get_api_by_name(selected_sub['panel'])
            client_found = False
            inbounds = api.inbound.get_list()
            for inbound in inbounds:
                for client in inbound.settings.clients:
                    if client.email == selected_email and client.tg_id == data.tg_id:
                        extend_subscription(client.email, client.id, 7, data.tg_id, client.sub_id, api)
                        extend_sub_panel_subscriptions(client.email, 7, int(data.tg_id), client.sub_id)
                        new_expiry = (datetime.now(timezone.utc) if selected_sub['is_expired'] else selected_sub['expiry_date']) + timedelta(days=7)
                        expiry_time = new_expiry.strftime("%Y-%m-%d %H:%M:%S")
                        await update_subscriptions_on_db(str(client.tg_id), selected_email, selected_sub['panel'], expiry_time, pool)
                        client_found = True
                        break
                if client_found:
                    break
            if not client_found:
                raise HTTPException(status_code=404, detail="Client not found")
            await apply_referral_bonus_db(str(data.tg_id), str(data.referee_id), pool)
            logger.info(f"Referral bonus extended subscription for tg_id: {data.tg_id}, email: {selected_email}, new_expiry: {expiry_time}")
            return {
                "email": selected_email,
                "panel": selected_sub['panel'],
                "expiry_date": expiry_time,
                "days": 7
            }
        else:
            # Условие 3: Требуется выбор подписки
            selected_email = data.email
            if not selected_email:
                raise HTTPException(status_code=400, detail="Email required for extension")
            selected_sub = next((sub for sub in non_trial_subs if sub['email'] == selected_email), None)
            if not selected_sub:
                raise HTTPException(status_code=404, detail="Subscription not found")
            api = get_api_by_name(selected_sub['panel'])
            client_found = False
            inbounds = api.inbound.get_list()
            for inbound in inbounds:
                for client in inbound.settings.clients:
                    if client.email == selected_email and client.tg_id == data.tg_id:
                        extend_subscription(client.email, client.id, 7, data.tg_id, client.sub_id, api)
                        extend_sub_panel_subscriptions(client.email, 7, int(data.tg_id), client.sub_id)
                        new_expiry = (datetime.now(timezone.utc) if selected_sub['is_expired'] else selected_sub['expiry_date']) + timedelta(days=7)
                        expiry_time = new_expiry.strftime("%Y-%m-%d %H:%M:%S")
                        await update_subscriptions_on_db(str(client.tg_id), selected_email, selected_sub['panel'], expiry_time, pool)
                        client_found = True
                        break
                if client_found:
                    break
            if not client_found:
                raise HTTPException(status_code=404, detail="Client not found")
            await apply_referral_bonus_db(str(data.tg_id), str(data.referee_id), pool)
            logger.info(f"Referral bonus extended subscription for tg_id: {data.tg_id}, email: {selected_email}, new_expiry: {expiry_time}")
            return {
                "email": selected_email,
                "panel": selected_sub['panel'],
                "expiry_date": expiry_time,
                "days": 7
            }
    except Exception as e:
        logger.error(f"Error applying referral bonus: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error applying referral bonus: {str(e)}")