"""Publish the owner's admin entry point once; never retries an uncertain send."""
import json
from pathlib import Path
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_sales_radar.config import OperatorBotSettings
from ai_sales_radar.operator_bot.callbacks import SignedCallbackCodec
from ai_sales_radar.operator_bot.navigation import menu_response


def main():
    settings = OperatorBotSettings.from_env()
    credentials = settings.require_credentials()
    receipt = Path('.local/admin-menu-receipt.json')
    if receipt.exists():
        raise SystemExit('Menu publication already attempted; inspect receipt before another send.')
    def call(method, payload):
        request = Request('https://api.telegram.org/bot' + credentials.bot_token + '/' + method,
                          data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
        with urlopen(request, timeout=30) as response:
            value = json.load(response)
        if not value.get('ok'):
            raise RuntimeError('Telegram operation rejected: ' + method)
        return value['result']
    scope = {'type': 'chat', 'chat_id': credentials.owner_chat_id}
    commands = [{'command': k, 'description': v} for k, v in [
        ('menu', 'Главное меню'), ('leads_menu', 'Лиды и фильтры'),
        ('sources_menu', 'Источники и поиск'), ('system_menu', 'Состояние и расходы AI'),
        ('help', 'Как пользоваться')]]
    call('setMyCommands', {'scope': scope, 'commands': commands})
    call('setChatMenuButton', {'chat_id': credentials.owner_chat_id, 'menu_button': {'type': 'commands'}})
    assert call('getMyCommands', {'scope': scope}) == commands
    assert call('getChatMenuButton', {'chat_id': credentials.owner_chat_id})['type'] == 'commands'
    menu = menu_response('home', SignedCallbackCodec(credentials.bot_token, ttl_seconds=settings.callback_ttl_seconds),
                         now=datetime.now(timezone.utc))
    receipt.parent.mkdir(exist_ok=True)
    receipt.write_text(json.dumps({'state': 'sending', 'commands_verified': True}))
    message = call('sendMessage', {'chat_id': credentials.owner_chat_id, 'text': menu.text,
        'parse_mode': 'HTML', 'reply_markup': {'inline_keyboard': [[
            {'text': b.label, 'callback_data': b.callback_data.decode()} for b in row] for row in menu.buttons]}})
    assert message['chat']['id'] == credentials.owner_chat_id
    assert len(message['reply_markup']['inline_keyboard']) == 2
    result = {'state': 'sent', 'commands_verified': True, 'menu_button_verified': True,
              'message_id': message['message_id'], 'rows': 2, 'buttons': 4}
    receipt.write_text(json.dumps(result, indent=2))
    print(json.dumps(result))

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(type(exc).__name__ + ': publication stopped; inspect local receipt; no automatic retry.', file=sys.stderr)
        sys.exit(1)
