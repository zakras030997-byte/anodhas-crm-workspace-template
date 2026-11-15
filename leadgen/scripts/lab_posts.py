"""Publish one approved package to the pinned laboratory; reuse session lifecycle."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telethon import types
from ai_sales_radar.channel_posts import ChannelPostService, ChannelPostError, _post_text, _image_path
from ai_sales_radar.config import TelegramAuthSettings

LAB_ID = 4324224634
OWNER_ID = 0

async def main(args):
    service = ChannelPostService(TelegramAuthSettings.from_env().require_credentials())
    async def operation(client):
        account = await client.get_me()
        if account.id != OWNER_ID or getattr(account, 'bot', False):
            raise ChannelPostError('Неверный аккаунт')
        dialogs = await client.get_dialogs(limit=200)
        matches = [d.entity for d in dialogs if getattr(d.entity, 'id', None) == LAB_ID]
        if len(matches) != 1:
            raise ChannelPostError('Точная Лаборатория не найдена')
        entity = matches[0]
        if not isinstance(entity, (types.Channel, types.Chat)) or 'лаборатор' not in entity.title.casefold():
            raise ChannelPostError('Тип или название Лаборатории не совпали')
        if not (getattr(entity, 'creator', False) or getattr(entity, 'admin_rights', None)):
            raise ChannelPostError('Права владельца/администратора не подтверждены')
        target = {'id': entity.id, 'title': entity.title, 'username': getattr(entity, 'username', None), 'owner': bool(getattr(entity, 'creator', False))}
        if args.command == 'check':
            print(json.dumps(target, ensure_ascii=False));return
        receipt = args.package / 'publication-receipt.json'
        if args.command == 'publish':
            if args.confirm_id != LAB_ID:
                raise ChannelPostError('ID не подтверждён')
            if receipt.exists():
                raise ChannelPostError('Попытка уже записана; повтор запрещён, используйте verify')
            text = _post_text((args.package/'FF-0911-LAB-post-v1.txt').read_text(), has_image=True)
            image = _image_path(args.package/'FF-0911-LAB-visual-v1.png')
            state = {'status':'sending','target':target,'text_sha256':hashlib.sha256(text.encode()).hexdigest()}
            receipt.write_text(json.dumps(state,ensure_ascii=False,indent=2))
            message = await client.send_file(entity, str(image), caption=text, parse_mode=None, force_document=False)
            state.update(status='sent', message_id=message.id)
            receipt.write_text(json.dumps(state,ensure_ascii=False,indent=2))
            print(json.dumps(state,ensure_ascii=False));return
        state=json.loads(receipt.read_text())
        if 'message_id' not in state:
            raise ChannelPostError('Нет квитанции отправки; нужна отдельная сверка истории')
        message=await client.get_messages(entity,ids=state['message_id'])
        expected=(args.package/'FF-0911-LAB-post-v1.txt').read_text().strip()
        if not message or message.message != expected or not message.photo:
            raise ChannelPostError('Текст или изображение не подтверждены')
        state.update(status='verified',image_verified=True,text_verified=True,
            url=f"https://t.me/c/{LAB_ID}/{message.id}")
        receipt.write_text(json.dumps(state,ensure_ascii=False,indent=2))
        print(json.dumps(state,ensure_ascii=False))
    await service._with_client(operation)

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=['check','publish','verify'])
    parser.add_argument('--package',type=Path,required=True)
    parser.add_argument('--confirm-id',type=int)
    args=parser.parse_args()
    try:
        asyncio.run(main(args))
    except ChannelPostError as exc:
        print(str(exc),file=sys.stderr);sys.exit(1)
    except Exception as exc:
        print(type(exc).__name__ + ': операция остановлена, проверьте квитанцию',file=sys.stderr);sys.exit(1)
