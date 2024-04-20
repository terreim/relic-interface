import cProfile
import pstats

import aiohttp
import asyncio

import requests
import re
from pymongo import MongoClient
from pymongo import UpdateOne
import time

class Initialize_Database():
    def __init__(self, client=MongoClient("mongodb://localhost:27017/")):
        self.client = client
        self.db = self.client["optimized_db"]
        self.prime_sets_collection = self.db["t_ps"]
        self.relics_collection = self.db["t_rc"]
        self.raw_collection = self.db["t_rd"]

class Data_Fetch_Conditions():
    async def __init__(self):
        self.not_set_pattern = re.compile(r".+(?<!kavasa_)prime_(?!set$).+$")
        self.set_pattern = re.compile(r".+prime_set$")
        self.relic_pattern = re.compile(r"^(?<!requiem_)[^r].+_relic$")

        self.sema = asyncio.BoundedSemaphore(3)
        self.delay = 0.5

        async with aiohttp.ClientSession() as self.session:
            self.items_data = await self.fetch_one(self.session, "https://api.warframe.market/v1/items", self.delay)
            self.raw_data = [(item["url_name"], item["id"]) for item in self.items_data["payload"]["items"]]
            self.filtered_data = [(name, _id) for (name, _id) in self.raw_data if self.set_pattern.match(name) or self.not_set_pattern.match(name) or self.relic_pattern.match(name)]

        self.database = Initialize_Database()

        # Set flags
        self.ps_not_corrupted = True
        self.ps_not_missing = True
        self.rc_not_corrupted = True
        self.rc_not_missing = True
        self.rd_not_corrupted = True
        self.rd_not_missing = True
        
        self.price_updated = True
        self.api_status = True

        # Price update for ps and rc?
        self.toggle_pu_ps = False
        self.toggle_pu_rc = False

        # Update the flags with values from data_check
        self.data_check()

    # Check if databases are updated and set manual toggle for price update 
    def data_check(self):
        ps_count = 0
        relic_count = 0
        pp_count = 0

        for item_name, _ in self.filtered_data:
            if self.set_pattern.match(item_name):
                ps_count += 1
            elif self.relic_pattern.match(item_name):
                relic_count += 1
            elif self.not_set_pattern.match(item_name):
                pp_count += 1
            
        raw_count = ps_count + relic_count + pp_count
        num_ps_db = self.database.prime_sets_collection.count_documents({})
        num_rc_db = self.database.relics_collection.count_documents({})
        num_rd_db = self.database.raw_collection.count_documents({})

        self.epoch_time = time.time()

        # Check rd DB
        if num_rd_db != raw_count:
            self.rd_not_corrupted = False
            self.price_updated = False
            self.rd_not_missing = num_rd_db != 0
            num_epoch_time = 0
        else:
            num_epoch_time = self.database.raw_collection.find_one()["epoch_t"]
        
        # Check ps DB
        if num_ps_db != ps_count:
            self.ps_not_corrupted = False
            self.ps_not_missing = num_ps_db != 0

        # Check rc DB
        if num_rc_db != relic_count:
            self.rc_not_corrupted = False
            self.rc_not_missing = num_rc_db != 0

        # Check price (can be set manually)
        if self.rd_not_corrupted:
            if self.epoch_time - num_epoch_time > 86400:
                self.price_updated = False

    def set_price_updated(self, value):
        self.price_updated = value

    #async funcs
    async def fetch_all(self, urls): 
        results = await asyncio.gather(*[self.fetch_one(url) for url in urls], return_exceptions=True)
        return results

    async def fetch_one(self, url):
        async with self.sema:  
            await asyncio.sleep(self.delay)
            async with self.session.get(url) as resp:
                print(resp)
                return await resp.json()
        
    # Only individual parts
    async def get_item_statistics(self, item_name):
        try:
            data = self.safe_get(f"https://api.warframe.market/v1/items/{item_name}/statistics")
            stats_closed = data["payload"]["statistics_closed"]["90days"]
            stats_live = data["payload"]["statistics_live"]["48hours"]
            
            prices_closed = [stat["avg_price"] for stat in stats_closed]
            prices_live = [stat["avg_price"] if stat["avg_price"] else 0 for stat in stats_live]
            
            average_closed = round(sum(prices_closed) / len(prices_closed), 2) if prices_closed else 0
            average_live = round(sum(prices_live) / len(prices_live), 2) if prices_live else 0
            
            return average_closed, average_live
            
        except requests.RequestException:
            return 0, 0

    def get_set_detail(self, set_name): # <- Get SET, not individual parts
        try:
            data = self.safe_get(f"https://api.warframe.market/v1/items/{set_name}")
            items_in_set = data["payload"]["item"]["items_in_set"]
            
            return [item for item in items_in_set if self.not_set_pattern.match(item["url_name"])]
        
        except requests.RequestException:
            self.api_status = False    
            return None

    def get_relic_detail(self, gen): # <- Expect a generator
        try:
            for ppn in gen:
                data = self.safe_get(f"https://api.warframe.market/v1/items/{ppn}/dropsources")
                relic_source = data["payload"]["dropsources"]
                
                yield [(relic_id, relic_data["rarity"]) 
                        for relic_data in relic_source 
                        for relic_id in relic_data["relic"].split(",")]
        
        except requests.RequestException:
            return []
        

class Database_Upload():
    def __init__(self):
        # Flags
        self.ps_not_corrupted = Data_Fetch_Conditions().ps_not_corrupted
        self.rc_not_corrupted = Data_Fetch_Conditions().rc_not_corrupted
        self.rd_not_corrupted = Data_Fetch_Conditions().rd_not_corrupted
        self.ps_not_missing = Data_Fetch_Conditions().ps_not_missing
        self.rc_not_missing = Data_Fetch_Conditions().rc_not_missing
        self.rd_not_missing = Data_Fetch_Conditions().rd_not_missing
        self.price_updated = Data_Fetch_Conditions().price_updated
        self.toggle_pu_ps = Data_Fetch_Conditions().toggle_pu_ps
        self.toggle_pu_rc = Data_Fetch_Conditions().toggle_pu_rc

        # Patterns
        self.set_pattern = Data_Fetch_Conditions().set_pattern
        self.not_set_pattern = Data_Fetch_Conditions().not_set_pattern
        self.relic_pattern = Data_Fetch_Conditions().relic_pattern
        self.epoch_time = Data_Fetch_Conditions().epoch_time
        
        self.database = Initialize_Database()
        self.item_info = Data_Fetch_Conditions().item_info

        self.get_item_statistics = Data_Fetch_Conditions().get_item_statistics
        self.get_set_detail = Data_Fetch_Conditions().get_set_detail
        self.get_relic_detail = Data_Fetch_Conditions().get_relic_detail


    # I should set manual toggles to allow for individual ps and relic price update. ATM I will just leave this be.
    def load_raw_and_price(self):
        if not self.rd_not_corrupted:   
            for raw, raw_id in self.item_info:
                if self.set_pattern.match(raw) or self.not_set_pattern.match(raw) or self.relic_pattern.match(raw):
                    price_90d, price_48h = self.get_item_statistics(raw)

                    new_data = {
                        "url_name": raw,
                        "price_90d": price_90d,
                        "price_48h": price_48h,
                        "epoch_t": self.epoch_time
                    }
                    if not self.rd_not_missing:
                        self.database.raw_collection.insert_one({"item_id": raw_id, **new_data})
                    else:
                        current_data = self.database.prime_sets_collection.find_one({"item_id": raw_id})
                        if current_data and all(current_data.get(key) == new_data.get(key) for key in new_data):
                            continue
                        self.database.prime_sets_collection.update_one(
                            {"item_id": raw_id},
                            {"$set": new_data}
                        )
            self.rd_not_corrupted = True
            self.rd_not_missing = True
            self.price_updated = True
        
        if not self.price_updated:
            for raw_item in self.database.raw_collection.find():
                price_90d, price_48h = self.get_item_statistics(raw_item["url_name"])
                raw_item["price_90d"] = price_90d
                raw_item["price_48h"] = price_48h 
                raw_item["epoch_t"] = self.epoch_time

                self.database.raw_collection.update_one({'_id': raw_item['_id']}, {'$set': raw_item})
            self.price_updated = True


    def load_prime_sets(self):
        if not self.ps_not_corrupted:
            for set_name, set_id in self.item_info:
                temp_list = []
                if self.set_pattern.match(set_name):
                    filtered_items = self.get_set_detail(set_name)
                    individual_part = [item["url_name"] for item in filtered_items]
                    set_doc = self.database.raw_collection.find_one({"item_id": set_id})

                    set_price_90 = set_doc.get("price_90d")
                    set_price_48 = set_doc.get("price_48h")

                    for list in self.get_relic_detail(part for part in individual_part):
                        ppn_sources_and_rarity = [
                            (name_.split(",")[0], part_rarity)
                            for ppn_source, part_rarity in list
                            for name_, id_ in self.item_info
                            if id_ in ppn_source
                        ]
                        temp_list.append(ppn_sources_and_rarity)

                    
                    new_data = {
                        "set_url": set_name,
                        "price_set": {"set_p90d": set_price_90,
                                    "set_p48h": set_price_48},
                        "parts_in_set": [
                            {
                                "item_url": item["url_name"],
                                "ducats": item["ducats"],
                                "item_id": item["id"],
                                "trading_tax": item["trading_tax"],
                                "quantity_for_set": item["quantity_for_set"],
                                "item_name": item["en"]["item_name"],
                                "price": {"price_90": p_90, "price_48": p_48},
                                "ppn_source_and_rarity": ppn_sources
                            }
                            for item, (p_90, p_48), ppn_sources in zip(filtered_items, [self.get_item_statistics(part) for part in individual_part], [source_list for source_list in temp_list])
                        ]
                        }

                    if not self.ps_not_missing:
                        self.database.prime_sets_collection.insert_one({"set_id": set_id, **new_data})
                    else:
                        current_data = self.database.prime_sets_collection.find_one({"set_id": set_id})
                        if current_data and all(current_data.get(key) == new_data.get(key) for key in new_data):
                            continue
                        self.database.prime_sets_collection.update_one(
                            {"set_id": set_id},
                            {"$set": new_data}
                        )
                
            self.ps_not_corrupted = True
            self.ps_not_missing = True

        if self.toggle_pu_ps:
            pipeline = [
                {
                    '$lookup': {
                        'from': 't_rd', 
                        'localField': 'set_id', 
                        'foreignField': 'item_id', 
                        'as': 'set_price_info'
                    }
                }, {
                    '$unwind': {
                        'path': '$set_price_info',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'price_set': {
                            'set_p90d': '$set_price_info.price_90d', 
                            'set_p48h': '$set_price_info.price_48h'
                        }
                    }
                }, {
                    '$unwind': '$parts_in_set'
                }, {
                    '$lookup': {
                        'from': 't_rd', 
                        'localField': 'parts_in_set.item_id', 
                        'foreignField': 'item_id', 
                        'as': 'parts_in_set.price_info'
                    }
                }, {
                    '$unwind': {
                        'path': '$parts_in_set.price_info',
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$addFields': {
                        'parts_in_set.price': {
                            'price_90': '$parts_in_set.price_info.price_90d', 
                            'price_48': '$parts_in_set.price_info.price_48h'
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$_id', 
                        'set_url': {
                            '$first': '$set_url'
                        }, 
                        'set_id': {
                            '$first': '$set_id'
                        }, 
                        'price_set': {
                            '$first': '$price_set'
                        }, 
                        'parts_in_set': {
                            '$push': '$parts_in_set'
                        }
                    }
                }, {
                    '$project': {
                        'parts_in_set.price_info': 0
                    }
                }]
            
            updated_ps = self.database.prime_sets_collection.aggregate(pipeline)
            bulk_operations = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": doc}
                ) for doc in updated_ps
            ]
            self.database.prime_sets_collection.bulk_write(bulk_operations)
                
            
    def load_relics(self):
        if not self.rc_not_corrupted:
            for relic_name, relic_id in self.item_info:
                if self.relic_pattern.match(relic_name):
                    relic_doc = self.database.raw_collection.find_one({"item_id": relic_id})

                    relic_p90d = relic_doc.get("price_90d")
                    relic_p48h = relic_doc.get("price_48h")
                    

                    reward_list = [({  
                            "part_url": part["item_url"],
                            "part_id": part["item_id"],
                            "ducats": part["ducats"],
                            "rarity": part_rarity,
                            "price_90d": part["price"]["price_90"],
                            "price_48h": part["price"]["price_48"]
                            })
                        for prime_set in self.database.prime_sets_collection.find()
                        for part in prime_set["parts_in_set"]
                        for relic_source, part_rarity in part.get("ppn_source_and_rarity", [])
                        if relic_name in relic_source
                        ]

                    if len(reward_list) == 5:
                        rarity_counts = {'common': 0, 'uncommon': 0}
                        for rarity in reward_list:
                            if rarity["rarity"] in rarity_counts:
                                rarity_counts[rarity["rarity"]] += 1

                        if rarity_counts['common'] < 3:
                            reward_list.append({"part_url": "forma_blueprint",
                                                "rarity": "common"})
                        elif rarity_counts['uncommon'] < 2:
                            reward_list.append({"part_url": "forma_blueprint",
                                                "rarity": "uncommon"})
            
                    new_data = {"relic_name": relic_name,
                                "relic_id": relic_id,
                                "relic_detail": {
                                "relic_p90d": relic_p90d, 
                                "relic_p48h": relic_p48h, 
                                "subtypes": "intact, exceptional, flawless, radiant",
                                "part_rewards": reward_list
                                }
                            }

                    if not self.rc_not_missing:
                        self.database.relics_collection.insert_one({"relic_id": relic_id, **new_data})
                    else:
                        current_data = self.database.relics_collection.find_one({"relic_id": relic_id})
                        if current_data and all(current_data.get(key) == new_data.get(key) for key in new_data):
                            continue
                        self.database.relics_collection.update_one(
                            {"relic_id": relic_id},
                            {"$set": new_data}
                        )
            self.rc_not_corrupted = True
            self.rc_not_missing = True    

        if self.toggle_pu_rc:
            pipeline = [
                    {
                        '$lookup': {
                            'from': 't_rd', 
                            'localField': 'relic_id', 
                            'foreignField': 'item_id', 
                            'as': 'relic_price_info'
                        }
                    }, {
                        '$unwind': {
                            'path': '$relic_price_info', 
                            'preserveNullAndEmptyArrays': True
                        }
                    }, {
                        '$addFields': {
                            'relic_detail.relic_p90d': '$relic_price_info.price_90d', 
                            'relic_detail.relic_p48h': '$relic_price_info.price_48h'
                        }
                    }, {
                        '$addFields': {
                            'filtered_part_rewards': {
                                '$filter': {
                                    'input': '$relic_detail.part_rewards', 
                                    'as': 'part', 
                                    'cond': {
                                        '$ifNull': [
                                            '$$part.part_id', False
                                        ]
                                    }
                                }
                            }
                        }
                    }, {
                        '$lookup': {
                            'from': 't_rd', 
                            'localField': 'filtered_part_rewards.part_id', 
                            'foreignField': 'item_id', 
                            'as': 'relic_price_info'
                        }
                    }, {
                        '$addFields': {
                            'relic_detail.part_rewards': {
                                '$map': {
                                    'input': '$relic_detail.part_rewards', 
                                    'as': 'part', 
                                    'in': {
                                        '$mergeObjects': [
                                            '$$part', {
                                                '$arrayElemAt': [
                                                    {
                                                        '$filter': {
                                                            'input': '$relic_price_info', 
                                                            'as': 'priceInfo', 
                                                            'cond': {
                                                                '$eq': [
                                                                    '$$priceInfo.item_id', '$$part.part_id'
                                                                ]
                                                            }
                                                        }
                                                    }, 0
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }, {
                        '$project': {
                            'relic_detail.part_rewards._id': 0, 
                            'relic_detail.part_rewards.item_id': 0, 
                            'relic_detail.part_rewards.url_name': 0, 
                            'relic_detail.part_rewards.epoch_t': 0, 
                            'filtered_part_rewards': 0, 
                            'relic_price_info': 0
                        }
                    }
                ]        
            updated_ps = self.database.relics_collection.aggregate(pipeline)
            bulk_operations = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": doc}
                ) for doc in updated_ps
            ]
            self.database.relics_collection.bulk_write(bulk_operations)
