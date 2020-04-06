import json
from datetime import datetime, timezone
import os
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import time

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
    total_count_limit = 10000
    # TODO: think about limit by time ( 2 days?)
    downloaded_messages = {}
    in_time_range = True

    while in_time_range:
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
                        or message.file.ext == ".pdf"
                    )
            ):

                if message.file.name not in os.listdir('../pdf_files'):
                    filename = await client.download_media(message=message, file='../pdf_files')
                else:
                    filename = message.file.name
                downloaded_messages[message.file.name] = message.date.strftime("%Y-%d-%m")
            else:
                filename = None

            message_dict = message.to_dict()
            if filename is not None:
                # TODO: add date to file_name
                message_dict['attached_file'] = filename

            all_messages.append(message_dict)
            if all_messages[-1]["date"] < datetime(2019,12,1,0,0,0, tzinfo=timezone.utc):
                in_time_range = False
                break

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

    with open(f'data/{channel_name}.json', 'w') as outfile:
        json.dump(all_messages, outfile, cls=DateTimeEncoder)

    with open(f'data/{channel_name}_DOWNLOADED.json', 'w') as outfile:
        json.dump(downloaded_messages, outfile, cls=DateTimeEncoder)


channels = [
    'MOHreport'
]
start_time = time.perf_counter()
for channel_name in channels:
    with client:
        client.loop.run_until_complete(main(channel_name))
end_time = time.perf_counter()
print(f"finished in {round(end_time - start_time)} seconds")