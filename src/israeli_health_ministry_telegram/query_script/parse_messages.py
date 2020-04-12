import json
import os
import pandas

corona_keys = ["corona", "covid"]
lockdown_keys = ["lockdown", "quarantine"]
spain_keys = ["spain", "spanish"]
italy_keys = ["italy", "italian"]


def has_keys(m, keys):
    return any([k.lower() in m["message"].lower() for k in keys])


all_messages = []
for filename in os.listdir("data"):
    if filename.endswith(".json"):
        print(filename)
        with open(f"data/{filename}", "r") as f:
            messages = json.load(f)
            for m in messages:
                if "message" not in m:
                    continue
                parsed_messaged = {
                    "channel_name": os.path.splitext(filename)[0],
                    "channel_id": m["to_id"]["channel_id"],
                    "id": m["id"],
                    "message": m["message"],
                }

                if "attached_file" in m:
                    parsed_messaged["attached_file"] = m["attached_file"]

                parsed_messaged["urls"] = []
                for entity in m["entities"]:
                    if "url" in entity:
                        parsed_messaged["urls"].append(entity["url"])

                all_messages.append(parsed_messaged)

print(len(all_messages))
# TODO:
with open(f"all_messages.json", "w") as f:
    json.dump(all_messages, f)

pandas.DataFrame(all_messages).to_csv("all_messages.csv")


# corona_messages = [m for m in all_messages if has_keys(m, corona_keys)]
# print(len(corona_messages))
# print(corona_messages[0])
# print(corona_messages[1])
# print(corona_messages[-1])
#
# lockdown_messages = [m for m in all_messages if has_keys(m, lockdown_keys)]
# print(len(lockdown_messages))
# print(lockdown_messages[0])
# print(lockdown_messages[1])
# print(lockdown_messages[-1])
#
#
# italy_messages = [m for m in all_messages if has_keys(m, italy_keys) and has_keys(m, lockdown_keys)]
# print(len(italy_messages))
# for m in italy_messages:
#     print(m)
