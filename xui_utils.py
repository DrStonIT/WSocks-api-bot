import logging
from py3xui import Api, Client
from datetime import datetime, timezone, timedelta
import config as cfg

PANELS = [

   {
        "name": "Panel1",
        "api": Api(host=cfg.PANEL1_HOST, username=cfg.PANEL1_USERNAME, password=cfg.PANEL1_PASSWORD, token=cfg.PANEL1_TOKEN),
        "create_key": lambda client: (
            f"vless://{client.id}@de-1.wsocks.ru:443?type=tcp&security=reality&pbk=c0DrIcQXeWqnmFysSVgfIVCcEr0LS_WJhlwxWsDnPWg&fp=chrome&sni=google.com&sid=bbdbd6f3&spx=%2F&flow=xtls-rprx-vision#WSocks VPN Germany"
         ),
       "create_link": lambda client: (
           f"https://agregator.wsocks.ru/sub/{client.sub_id}/WSocks"
       )
     }
    # {
    #     "name": "Panel2",
    #     "api": Api(host=cfg.PANEL2_HOST, username=cfg.PANEL2_USERNAME, password=cfg.PANEL2_PASSWORD, token=cfg.PANEL2_TOKEN),
    #     "create_key": lambda client: (
    #         f"vless://{client.id}@de-2.wsocks.ru:443?type=tcp&security=reality&pbk=s"
    #         f"-R4V_XUgnbRlLLCtqri10dcdd1QLNEAU6B04LpRX3U&fp=chrome&sni=google.com&sid=5f&spx=%2F&flow=xtls-rprx-vision#WSocks VPN Germany"
    #     )
    # },
    # {
    #     "name": "Panel3",
    #     "api": Api(host=cfg.PANEL3_HOST, username=cfg.PANEL3_USERNAME, password=cfg.PANEL3_PASSWORD, token=cfg.PANEL3_TOKEN),
    #     "create_key": lambda client: (
    #         f"vless://{client.id}@de-3.wsocks.ru:443?type=tcp&security=reality&pbk"
    #         f"=MCEDsjvqBrJGLXk-yJOsSu5-RK8fO7kkFT_RC_giNgM&fp=chrome&sni=google.com&sid=8e&spx=%2F&flow=xtls-rprx-vision#WSocks VPN Germany"
    #     ),
    #     "create_link": lambda client: (
    #       f"https://de-3.wsocks.ru:2096/SubWSocks_VPN_DE_FRA-3/{client.sub_id}"
    #   )
    # }
]

for panel in PANELS:
    panel["api"].login()

def get_panel_load(api):
    try:
        inbounds = api.inbound.get_list()
        total_clients = sum(len(inbound.settings.clients) for inbound in inbounds)
        return total_clients
    except Exception as e:
        logging.error(f"Ошибка при получении нагрузки панели: {e}")
        return float("inf")

def get_best_panel():
    suitable_panel = None
    min_load = float("inf")
    for panel in PANELS:
        load = get_panel_load(panel["api"])
        if load < min_load:
            min_load = load
            suitable_panel = panel
    return suitable_panel

def get_api_by_name(name):
    panel = next((panel for panel in PANELS if panel['name'] == name), None)
    return panel['api'] if panel else None

def get_active_subscriptions(tg_id):
    subscriptions = []
    for panel in PANELS:
        try:
            inbounds = panel["api"].inbound.get_list()
            for inbound in inbounds:
                for client in inbound.settings.clients:
                    if client.tg_id == tg_id:
                        expiry_date = datetime.fromtimestamp(client.expiry_time / 1000.0, tz=timezone.utc)
                        subscriptions.append({
                            "email": client.email,
                            "id": client.id,
                            "key": panel["create_key"](client),
                            "sub_link": panel["create_link"](client),
                            "expiry_date": expiry_date,
                            "sub_id": client.sub_id,
                            "is_expired": expiry_date <= datetime.now(timezone.utc),
                            "panel": panel["name"]
                        })

        except Exception as e:
            logging.error(f"Ошибка при проверке подписок на {panel['name']}: {e}")
    return subscriptions

def extend_subscription(user_email: str, user_uuid: str, days_extension: int, tg_id, subscription_id, api):
    try:
        client = api.client.get_by_email(user_email)
        if not client:
            print(f"Ошибка: клиент с Email {user_email} не найден.")
            return
        current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        if client.expiry_time < current_time:
            new_expiry_time = current_time + int(timedelta(days=days_extension).total_seconds() * 1000)
        else:
            new_expiry_time = client.expiry_time + int(timedelta(days=days_extension).total_seconds() * 1000)
        client.expiry_time = new_expiry_time
        client.id = user_uuid
        client.tg_id = tg_id
        client.flow = "xtls-rprx-vision"
        client.enable = True
        client.limit_ip = 5
        client.sub_id = subscription_id
        api.client.update(user_uuid, client)
        print(f"Подписка {client.email} успешно продлена.")
    except Exception as e:
        print(f"Ошибка при продлении подписки: {e}")

def delete_trial_subscription(panel, email):
    api = get_api_by_name(panel)
    inbounds = api.inbound.get_list()
    for inbound in inbounds:
        for client in inbound.settings.clients:
            if client.email == email:
                api.client.delete(1, client.id)
    logging.info(f"Удалена пробная подписка {email} с панели {panel}.")

def delete_subscriptions(panel, email):
    api = get_api_by_name(panel)
    inbounds = api.inbound.get_list()
    for inbound in inbounds:
        for client in inbound.settings.clients:
            if client.email == email and ("DE-FRA-USER" in email or "DE-FRA-TRIAL" in email):
                api.client.delete(1, client.id)
    logging.info(f"Удалена подписка {email} с панели {panel}.")
