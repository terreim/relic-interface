import re
import pprint

import cProfile
import pstats
import aiohttp
import asyncio
from pymongo import MongoClient
import time

    
client = MongoClient("mongodb://localhost:27017/")
db = client["optimized_db"]
prime_sets_collection = db["t_ps"]
relics_collection = db["t_rc"]
raw_db = db["t_rd"]

prime_pattern = re.compile(r".+prime_set$")
not_set_pattern = re.compile(r".+(?<!kavasa_)prime_(?!set$).+$")
relic_pattern = re.compile(r"^(?<!requiem_)[^r].+_relic$")

sema = asyncio.BoundedSemaphore(3)

async def fetch_all(session, urls, delay): 
    results = await asyncio.gather(*[fetch_one(session, url, delay) for url in urls], return_exceptions=True)
    return results

async def fetch_one(session, url, delay):
    async with sema:  
        await asyncio.sleep(delay)
        async with session.get(url) as resp:
            print(resp)
            return await resp.json()

async def main():
    delay = 0.5
    async with aiohttp.ClientSession() as session:
        items_data = await fetch_one(session, "https://api.warframe.market/v1/items", delay)
        print(items_data)
        raw_data = [(item["url_name"], item["id"]) for item in items_data["payload"]["items"]]
        filtered_data = [(name, _id) for (name, _id) in raw_data if prime_pattern.match(name) or not_set_pattern.match(name) or relic_pattern.match(name)]

        urls = [f"https://api.warframe.market/v1/items/{name}/statistics" for name, _id in filtered_data]
        results = await fetch_all(session, urls, delay)

        epoch_time = time.time()
        price_lists = []
        for result in results:
            if isinstance(result, Exception):
                continue 
            stats_closed = result["payload"]["statistics_closed"]["90days"]
            stats_live = result["payload"]["statistics_live"]["48hours"]

            prices_closed = [stat["avg_price"] for stat in stats_closed]
            prices_live = [stat["avg_price"] if stat["avg_price"] else 0 for stat in stats_live]
            
            price_90d = round(sum(prices_closed) / len(prices_closed), 2) if prices_closed else 0
            price_48h = round(sum(prices_live) / len(prices_live), 2) if prices_live else 0
            
            price_lists.append((price_90d, price_48h))

        for (name, _id), (price_90d, price_48h) in zip(filtered_data, price_lists):
            new_data = {
                "url_name": name,
                "item_id": _id,
                "price_90d": price_90d,
                "price_48h": price_48h,
                "epoch_t": epoch_time
            }
            raw_db.insert_one(new_data)

# Profiling
profiler = cProfile.Profile()
profiler.enable()
asyncio.run(main())  # Properly calling the asynchronous main function
profiler.disable()

stats = pstats.Stats(profiler)
stats.sort_stats('cumulative').print_stats(30)

# async def fetch_json(q: asyncio.Queue, session: aiohttp.ClientSession):
#     while True:
#         async with sema: 
#             url = await q.get()
#             response = await session.request(url)
#             try:
#                 response.raise_for_status()
#                 if response.status == 200:
#                     return await response.json()
#             except Exception as e: 
#                 if response.status == 429:
#                     await asyncio.sleep(5)
#                     return await fetch_json(url, session)
#                 elif response.status != 200:
#                     raise Exception(f"HTTP error: {response.status}")
#                 else:
#                     print(f"Error: {e}")
#         q.task_done()

# async def main():
#     async with aiohttp.ClientSession() as session:
#         q = asyncio.Queue()
#         epoch_time = time.time()

#         await q.put("https://api.warframe.market/v1/items")
#         items_data = await fetch_json(q, session)  

#         raw_data = [(item["url_name"], item["id"]) for item in items_data["payload"]["items"]]
#         filtered_data = [(name, _id) for (name, _id) in raw_data if prime_pattern.match(name) or not_set_pattern.match(name) or relic_pattern.match(name)]
        
#         list_url = []
#         valid_name = []
#         for name, _id in filtered_data:
#             url = f"https://api.warframe.market/v1/items/{name}/statistics"
#             list_url.append(url)
#             valid_name.append((name, _id))

#         for url in list_url:
#             await q.put(url)
        
#         tasks = [asyncio.create_task(fetch_json(q, session)) for _ in range(len(list_url))]
#         results = await asyncio.gather(*tasks)

#         for result in results:
#             stats_closed = result["payload"]["statistics_closed"]["90days"]
#             stats_live = result["payload"]["statistics_live"]["48hours"]

#             prices_closed = [stat["avg_price"] for stat in stats_closed]
#             prices_live = [stat["avg_price"] if stat["avg_price"] else 0 for stat in stats_live]
            
#             price_90d = round(sum(prices_closed) / len(prices_closed), 2) if prices_closed else 0
#             price_48h = round(sum(prices_live) / len(prices_live), 2) if prices_live else 0
            

#             for name, _id in valid_name:
#                 new_data = {
#                     "url_name": name,
#                     "item_id": _id,
#                     "price_90d": price_90d,
#                     "price_48h": price_48h,
#                     "epoch_t": epoch_time
#                 }

#                 raw_db.insert_one(new_data)

#         for c in results:
#             c.cancel()

# for set in relics_collection.find():
#     pprint.pp(set)

# profiler = cProfile.Profile()
# profiler.enable()
# asyncio.run(main())
# profiler.disable()

# stats = pstats.Stats(profiler)
# stats.sort_stats('cumulative').print_stats(30)


# async def fetch_json(session, url):
#     async with session.get(url, timeout=60) as response:
#         return await response.json()   

# async def get_item_statistics(session, item_name):
#     try:
#         response = await async_host(session, f"https://api.warframe.market/v1/items/{item_name}/statistics")

#         stats_closed = response["payload"]["statistics_closed"]["90days"]
#         stats_live = response["payload"]["statistics_live"]["48hours"]

#         prices_closed = [stat["avg_price"] for stat in stats_closed]
#         prices_live = [stat["avg_price"] if stat["avg_price"] else 0 for stat in stats_live]
        
#         average_closed = round(sum(prices_closed) / len(prices_closed), 2) if prices_closed else 0
#         average_live = round(sum(prices_live) / len(prices_live), 2) if prices_live else 0
        
#         return average_closed, average_live
#     except Exception as e:
#         print(f"Error: {e}")
#         return 0, 0

# async def load_data(rate_limit_per_second=1):
#     async with aiohttp.ClientSession() as session:
#         epoch_time = time.time()
#         items_data = await async_host(session, "https://api.warframe.market/v1/items")
#         item_name = [(item["url_name"], item["id"]) for item in items_data["payload"]["items"]]

#         delay = 1 / rate_limit_per_second

#         tasks = []
#         valid_items = []
#         for name, _id in item_name:
#             if prime_pattern.match(name) or not_set_pattern.match(name) or relic_pattern.match(name):
#                 url = f"https://api.warframe.market/v1/items/{name}/statistics"
#                 task = paced_fetch(session, url, delay)
#                 tasks.append(task)
#                 valid_items.append((name, _id))

#         results = await asyncio.gather(*tasks)

#         for (name, _id), (price_90d, price_48h) in zip(valid_items, results):
#             new_data = {
#                 "url_name": name,
#                 "price_90d": price_90d,
#                 "price_48h": price_48h,
#                 "epoch_t": epoch_time
#             }

#             raw_db.insert_one(new_data)

#         if prime_pattern.match(p_set):
#             item_set = await async_host(session, f"https://api.warframe.market/v1/items/{p_set}")
#             filtered_items = [item for item in item_set["payload"]["item"]["items_in_set"] if not_set_pattern.match(item["url_name"])]
#             individual_part = [item["url_name"] for item in filtered_items]

#             set_price_90, set_price_48 = await get_item_statistics(session, p_set)
            
#             temp_list = [await collect_async_gen(get_relic_detail(session, part)) for part in individual_part]
            
#             parts = {
#                 "set_url": p_set,
#                 "set_id": _id,
#                 "price_set": {"set_p90d": set_price_90, "set_p48h": set_price_48},
#                 "parts_in_set": [
#                     {
#                         "url_name": item["url_name"],
#                         "ducats": item["ducats"],
#                         "item_id": item["id"],
#                         "trading_tax": item["trading_tax"],
#                         "quantity_for_set": item["quantity_for_set"],
#                         "item_name": item["en"]["item_name"],
#                         "price": {"price_90": p_90, "price_48": p_48},
#                         "ppn_source_and_rarity": ppn_sources
#                     } for item, (p_90, p_48), ppn_sources in zip(filtered_items, [await get_item_statistics(session, part) for part in individual_part], temp_list)
#                 ]
#             }

#             prime_sets_collection.insert_one(parts)
            
#         dump_dicts = {}
#         if relic_pattern.match(p_set):
#             relic_p90d, relic_p48h = await get_item_statistics(p_set)

#             reward_dict = {
#                 part["url_name"]: {
#                     "ducats": part["ducats"],
#                     "rarity": part_rarity,
#                     "price_90d": part["price"]["price_90"],
#                     "price_48h": part["price"]["price_48"]
#                 }
#                 for prime_set in prime_sets_collection.find()
#                 for part in prime_set["parts_in_set"]
#                 for relic_source, part_rarity in part.get("ppn_source_and_rarity", [])
#                 if p_set in relic_source
#                 }
            
#             dump_dicts = {"relic_name": p_set,
#                         "relic_id": _id,
#                         "relic_detail": {
#                             "relic_p90d": relic_p90d, 
#                             "relic_p48h": relic_p48h, 
#                             "subtypes": "intact, exceptional, flawless, radiant",
#                             "part_rewards": reward_dict
#                             }
#                         }
            
#             relics_collection.insert_one(dump_dicts)

    
        
        
        
#         if len(dump_dicts["relic_detail"]["part_rewards"]) <= 5:
#             rarity_counts = {'common': 0, 'uncommon': 0, 'rare': 0}
#             part_rewards = dump_dicts["relic_detail"]["part_rewards"]
            
#             for part_url, part_info in part_rewards.items():
#                 if part_info["rarity"] in rarity_counts:
#                     rarity_counts[part_info["rarity"]] += 1

#             if rarity_counts['common'] < 3:
#                 part_rewards["forma_blueprint"] = {"rarity": "common"}
#             elif rarity_counts['uncommon'] < 2:
#                 part_rewards["forma_blueprint"] = {"rarity": "uncommon"}

# for relic_doc in relics_collection.find():
#     r_90, r_48 = get_item_statistics(relic_doc["relic_name"])
#     relic_doc["relic_detail"]["relic_p90d"] = r_90
#     relic_doc["relic_detail"]["relic_p48h"] = r_48
#     print(r_90)
#     print(r_48)
#     for part_name, part_info in relic_doc["relic_detail"]["part_rewards"].items():
#         print(part_name)
    
# epoch_t = time.time()
# for raw, _id in item_name:
#     raw_json = {}
#     if prime_pattern.match(raw) or not_set_pattern.match(raw) or relic_pattern.match(raw):
#         price_90d, price_48h = get_item_statistics(raw)

#         raw_json = {
#             "url_name": raw,
#             "item_id": _id,
#             "price_90d": price_90d,
#             "price_48h": price_48h,
#             "epoch_t": epoch_t
#         }

#         raw_db.insert_one(raw_json)

# for item_name in raw_db.find():
#     if not re.match(r".+(?<!kavasa_)prime_(?!set$).+$", item_name["url_name"]):
#         print(item_name["url_name"])
#         time.sleep(0.5)

# for relic_name, relic_id in item_name:
#     if relic_pattern.match(relic_name):
#         relic_p90d, relic_p48h = get_item_statistics(relic_name)
        
#         reward_list = [
#             ({  "part_url": part["item_url"],
#                 "part_id": part["item_id"],
#                 "ducats": part["ducats"],
#                 "rarity": part_rarity,
#                 "price_90d": part["price"]["price_90"],
#                 "price_48h": part["price"]["price_48"]
#             })
#             for prime_set in prime_sets_collection.find()
#             for part in prime_set["parts_in_set"]
#             for relic_source, part_rarity in part.get("ppn_source_and_rarity", [])
#             if relic_name in relic_source
#         ]
        
#         if len(reward_list) == 5:
#             rarity_counts = {'common': 0, 'uncommon': 0}
#             for rarity in reward_list:
#                 if rarity["rarity"] in rarity_counts:
#                     rarity_counts[rarity["rarity"]] += 1

#             if rarity_counts['common'] < 3:
#                 reward_list.append({"forma_blueprint" : {"rarity": "common"}})
#             elif rarity_counts['uncommon'] < 2:
#                 reward_list.append({"forma_blueprint" : {"rarity": "uncommon"}})
    
#         new_data = {"relic_name": relic_name,
#                     "relic_id": relic_id,
#                     "relic_detail": {
#                         "relic_p90d": relic_p90d, 
#                         "relic_p48h": relic_p48h, 
#                         "subtypes": "intact, exceptional, flawless, radiant",
#                         "part_rewards": reward_list
#                         }
#                     }
                
#         pprint.pp(new_data)