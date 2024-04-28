import requests
import re
from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["test_static_data"]
prime_parts_collection = db["t_ppc"]
relics_collection = db["t_rc"]
relic_rewards = db["t_rr"]
raw_data = db["t_rd"]

session = requests.Session()
headers = {"Content-Type": "application/json"}

def safe_get(url, **kwargs):
    try:
        return session.get(url, headers=headers, **kwargs).json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None


items_data = safe_get("https://api.warframe.market/v1/items")
if items_data:
    item_info = [(item["url_name"], item["id"]) for item in items_data["payload"]["items"]]
    count = raw_data.count_documents({})
    if count != len(item_info):
        raw_data.delete_many({})
        raw_data.insert_many(items_data["payload"]["items"])
else:
    item_info = []


def get_items_price(item_name):
    data = safe_get(f"https://api.warframe.market/v1/items/{item_name}/statistics")
    if data:
        stats_closed = data["payload"]["statistics_closed"]["90days"]
        stats_live = data["payload"]["statistics_live"]["48hours"]
        return ([stat["avg_price"] for stat in stats_closed],
                [stat["avg_price"] if stat["avg_price"] else 0 for stat in stats_live])
    return [], []

def get_item_detail(item_name, detail):
    data = safe_get(f"https://api.warframe.market/v1/items/{item_name}")
    if data:
        items_in_set = data["payload"]["item"]["items_in_set"]
        return next((item[detail] for item in items_in_set if item["url_name"] == item_name), None)
    return None

def get_item_ducats(item_name):
    return get_item_detail(item_name, "ducats")

def get_thumb(item_name):
    return get_item_detail(item_name, "thumb")


def get_relic_detail(ppn):
    data = safe_get(f"https://api.warframe.market/v1/items/{ppn}/dropsources")
    if data:
        relic_source = data["payload"]["dropsources"]
        return [(relic_id, relic_data["rarity"]) 
                for relic_data in relic_source 
                for relic_id in relic_data["relic"].split(",")]
    return []


def main():
    for item_name, item_id in item_info:

        if re.match(r".+(?<!kavasa_)prime_(?!set$).+$", item_name):
            prices90, prices48 = get_items_price(item_name)
            
            prices_90days = round(sum(prices90)/len(prices90), 2) if prices90 else 0
            prices_48hours = round(sum(prices48)/len(prices48), 2) if prices48 else 0

            ducat_value = get_item_ducats(item_name)
            thumb_url = get_thumb(item_name)
            ppn_sources = get_relic_detail(item_name)

            ppn_sources_and_rarity = [(relic_ppn_name, part_rarity) 
                                        for ppn_source, part_rarity in ppn_sources
                                        for name_, id_ in item_info 
                                        for relic_ppn_name in name_.split(",") 
                                        if id_ in ppn_source]
            nested_ppn_sources = {relic_ppn_name: part_rarity for relic_ppn_name, part_rarity in ppn_sources_and_rarity}

            prime_parts_collection.insert_one({"item_id": item_id, 
                                                "item_url": item_name,
                                                "prices_90days": prices_90days, 
                                                "prices_48hours": prices_48hours, 
                                                "ducat_value": ducat_value,
                                                "thumb_url": thumb_url,
                                                "ppn_sources_and_rarity": nested_ppn_sources
                                                })
            

        if re.match(r"^(?<!requiem_)[^r].+_relic$", item_name):
            prices90, prices48 = get_items_price(item_name)
            
            prices_90days = round(sum(prices90)/len(prices90), 2) if prices90 else 0
            prices_48hours = round(sum(prices48)/len(prices48), 2) if prices48 else 0
            thumb_url = get_thumb(item_name)

            relics_collection.insert_one({"item_id": item_id, 
                                          "item_url": item_name,
                                          "prices_90days": prices_90days, 
                                          "prices_48hours": prices_48hours, 
                                          "thumb_url": thumb_url,
                                          "subtypes": "intact, exceptional, flawless, radiant"
                                        })

    for relic_doc in relics_collection.find():
        relic_name = relic_doc["item_url"]
        pipeline = [
        {
            '$match': {
                f'ppn_sources_and_rarity.{relic_name}': {
                    '$exists': True
                }
            }
        }, {
            '$group': {
                '_id': relic_name, 
                'url_names': {
                    '$push': '$item_url'
                }, 
                'value': {
                    '$push': f'$ppn_sources_and_rarity.{relic_name}'
                }
            }
        }, {
            '$project': {
                'tuple_value': {
                    '$arrayToObject': {
                        '$zip': {
                            'inputs': [
                                '$url_names', '$value'
                            ]
                        }
                    }
                }
            }
        }
    ]
        relic_list = list(prime_parts_collection.aggregate(pipeline))
        if relic_list:
            for relic_data in relic_list:
                rarity_counts = {'common': 0, 'uncommon': 0, 'rare': 0}

                for rarity in relic_data["tuple_value"].values():
                    if rarity in rarity_counts:
                        rarity_counts[rarity] += 1

                if rarity_counts['common'] < 3:
                    relic_data["tuple_value"]["forma_blueprint"] = "common"
                elif rarity_counts['uncommon'] < 2:
                    relic_data["tuple_value"]["forma_blueprint"] = "uncommon"

                relic_rewards.insert_one(relic_data)
        else:
            continue


if __name__ == "__main__":
    main()