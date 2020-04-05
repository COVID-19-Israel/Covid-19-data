import json
from datetime import datetime, timezone
import os
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

with open("personal_data/personal_data.txt", mode="r") as file:

    personal_data = file.readlines()
    api_id = int(personal_data[0])
    api_hash = personal_data[1]
    phone_number = personal_data[2]
    client = TelegramClient(session='mysession', api_id=api_id, api_hash=api_hash)


async def main(channel_name):
    await client.start(phone=lambda: [phone_number])
    print("Client Created")

    channel_url = f'https://t.me/{channel_name}'
    print(f'Channel URL: {channel_url}')
    channel = await client.get_entity(channel_url)

    offset_id = 0
    all_messages = []
    downloaded_messages = {}
    total_count_limit = 10000
    # TODO: think about limit by time ( 2 days?)

    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=100,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break

        for message in history.messages:
            if (message.file is not None
                    and message.file.name
                    and message.file.size <= 10000000
                    and (
                            message.file.name.startswith('מכלול_אשפוז_דיווח')
                        or message.file.ext == "pdf"
                    )
            ):
                filename = await client.download_media(message=message, file='../pdf_files')
                downloaded_messages[message.file.name] = message.file.date.strftime("%Y-%m-%d")
            else:
                filename = None

            message_dict = message.to_dict()
            if filename is not None:
                # TODO: add date to file_name
                message_dict['attached_file'] = filename

            all_messages.append(message_dict)

        offset_id = all_messages[-1]['id']
        print(f'Date: {all_messages[-1]["date"]}')
        print(f'Total messages: {len(all_messages)}/{total_count_limit}')

        if len(all_messages) >= total_count_limit:
            break

    # class to parse datetime to json
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime):
                return o.isoformat()

            if isinstance(o, bytes):
                return list(o)

            return json.JSONEncoder.default(self, o)

    with open(f'../data/{channel_name}.json', 'w') as outfile:
        json.dump(all_messages, outfile, cls=DateTimeEncoder)

    with open(f'../data/{channel_name}_DOWNLOADED.json', 'w') as outfile:
        json.dump(downloaded_messages, outfile, cls=DateTimeEncoder)


channels = [
    'MOHreport'
]

for channel_name in channels:
    with client:
        client.loop.run_until_complete(main(channel_name))

