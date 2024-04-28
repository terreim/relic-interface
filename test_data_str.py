
import pprint
import aiohttp
import asyncio
import re
from pymongo import MongoClient
from pymongo import UpdateOne, InsertOne
import time

class Initialize_Database():
    def __init__(self, client=MongoClient("mongodb+srv://myAtlasDBUser:19-91Minutes@atlascluster.kcbmbec.mongodb.net/")):
        self.client = client
        self.db = self.client["optimized_db"]
        self.prime_sets_collection = self.db["t_ps"]
        self.relics_collection = self.db["t_rc"]
        self.raw_collection = self.db["t_rd"]

class Data_Fetch_Conditions():
    def __init__(self):
        self.not_set_pattern = re.compile(r".+(?<!kavasa_)prime_(?!set$).+$")
        self.set_pattern = re.compile(r"^(.+)_prime_set$")
        self.relic_pattern = re.compile(r"^(?<!requiem_)[^r].+_relic$")

        # Asycn stuffs
        self.sema = asyncio.BoundedSemaphore(3)
        self.delay = 0.7
        self.session = None
        self.filtered_data = None

        # DBs
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

    async def async_init(self, session):
        self.session = session
        self.items_data = await self.fetch_one("https://api.warframe.market/v1/items")
        self.raw_data = [(item["url_name"], item["id"]) for item in self.items_data["payload"]["items"]]
        self.filtered_data = [
            (name, _id) for (name, _id) in self.raw_data 
            if self.set_pattern.match(name) or self.not_set_pattern.match(name) or self.relic_pattern.match(name)
        ]
        # Update the flags with values from data_check
        await self.data_check()
        return self

    # Check if databases are updated and set manual toggle for price update 
    async def data_check(self):
        ps_count = relic_count = pp_count = 0

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
        
        # Check ps DB
        if num_ps_db != ps_count:
            self.ps_not_corrupted = False
            self.ps_not_missing = num_ps_db != 0

        # Check rc DB
        if num_rc_db != relic_count:
            self.rc_not_corrupted = False
            self.rc_not_missing = num_rc_db != 0

        # Check price (can be set manually)
        num_epoch_time = self.database.raw_collection.find_one(sort=[("epoch_t", -1)])["epoch_t"] if self.rd_not_corrupted else 0
        if self.epoch_time - num_epoch_time > 86400:
            self.price_updated = False

    def set_price_updated(self, value):
        self.price_updated = value
    
    def toggle_price_ps(self, value):
        self.toggle_pu_ps = value
    
    def toggle_price_rc(self, value):
        self.toggle_pu_rc = value

    # Async methods
    async def fetch_all(self, urls): 
        results = await asyncio.gather(*[self.fetch_one(url.split(",")[0]) for (_, url) in urls], return_exceptions=True)
        return results

    async def fetch_one(self, url):
        async with self.sema:  
            await asyncio.sleep(self.delay)
            async with self.session.get(url) as resp:
                return await resp.json()
            
    async def close(self):
        await self.session.close()
            
    def calculate_average_price(self, stats):
        prices = [stat["avg_price"] for stat in stats if stat["avg_price"] is not None]
        return round(sum(prices) / len(prices), 2) if prices else 0   
         
    def process_statistics(self, result):
        stats_closed = result["payload"]["statistics_closed"]["90days"]
        stats_live = result["payload"]["statistics_live"]["48hours"]
        price_90d = self.calculate_average_price(stats_closed)
        price_48h = self.calculate_average_price(stats_live)
        return price_90d, price_48h

    async def get_relic_detail(self, gen): #Generator
        try:
            for ppn in gen:
                data = self.fetch_all(f"https://api.warframe.market/v1/items/{ppn}/dropsources")
                relic_source = data["payload"]["dropsources"]
                
                yield [(relic_id, relic_data["rarity"]) 
                        for relic_data in relic_source 
                        for relic_id in relic_data["relic"].split(",")]
        
        except aiohttp.ClientError:
            yield []
        

class Database_Upload():
    def __init__(self):
        self.init_DFC = None
        
    async def instantiate_DFC(self):
        session = aiohttp.ClientSession()
        dfc = Data_Fetch_Conditions()
        self.init_DFC = await dfc.async_init(session)

    async def load_raw_and_price(self):
        if not self.init_DFC.rd_not_corrupted:
            filtered_raw = [(name, _id) for name, _id in self.init_DFC.filtered_data]

            if self.init_DFC.rd_not_missing:
                existing_data = [raw["url_name"] for raw in self.init_DFC.database.raw_collection.find()]
                filtered_raw = [(name, _id) for name, _id in filtered_raw if name not in existing_data]

            urls = [(name, f"https://api.warframe.market/v1/items/{name}/statistics") for name, _id in filtered_raw]
            results = await self.init_DFC.fetch_all(urls)

            updates = []
            bulk_ops = []
            for result in results:
                if isinstance(result, Exception):
                    print(f"Error: {repr(result)}")
                price_90d, price_48h = self.init_DFC.process_statistics(result)
                updates.append((price_90d, price_48h))

            for (name, _id), (price_90d, price_48h) in zip(filtered_raw, updates):
                new_data = {
                    "url_name": name,
                    "price_90d": price_90d,
                    "price_48h": price_48h,
                    "epoch_t": self.init_DFC.epoch_time
                }
                if not self.init_DFC.rd_not_missing:
                    existing_item = self.init_DFC.database.raw_collection.find_one({"item_id": _id})
                    if existing_item is None:
                        bulk_ops.append(InsertOne({"item_id": _id, **new_data}))
                    else:
                        bulk_ops.append(UpdateOne({"item_id": _id}, {"$set": new_data}))

            if bulk_ops:
                self.init_DFC.database.raw_collection.bulk_write(bulk_ops)

            self.init_DFC.rd_not_corrupted = True
            self.init_DFC.rd_not_missing = True
            self.init_DFC.price_updated = True
        
        if not self.init_DFC.price_updated:
            urls = [f"https://api.warframe.market/v1/items/{name}/statistics" for name, _id in self.init_DFC.filtered_data]
            results = await self.init_DFC.fetch_all(urls)

            updates = []
            for result in results:
                if isinstance(result, Exception):
                    print(f"Error: {repr(result)}")
                    continue
                price_90d, price_48h = self.init_DFC.process_statistics(result)
                updates.append((price_90d, price_48h))

            bulk_ops = [UpdateOne({"item_id": _id}, 
                                {"$set": {
                                        "url_name": name,
                                        "price_90d": price_90d, 
                                        "price_48h": price_48h, 
                                        "epoch_t": self.init_DFC.epoch_time
            }}) for (name, _id), (price_90d, price_48h) in zip(self.init_DFC.filtered_data, updates)]
            
            self.init_DFC.database.raw_collection.bulk_write(bulk_ops)                                                     
            self.init_DFC.price_updated = True                                                       
    

    async def load_prime_sets(self):
        if not self.init_DFC.ps_not_corrupted:
            filtered_raw = {(name, _id) for name, _id in self.init_DFC.filtered_data if self.init_DFC.set_pattern.match(name)}

            if self.init_DFC.ps_not_missing:
                existing_sets = {set["set_url"]: set for set in self.init_DFC.database.prime_sets_collection.find()}
                filtered_raw = {(name, _id) for name, _id in filtered_raw if name not in existing_sets}

            name_set = set()
            for name, _id in filtered_raw:
                match = self.init_DFC.set_pattern.match(name)
                name_set.add(match.group(1))
            
            bulk_ops = []
            for name in name_set:
                temp_list = []
                temp_data = {}
                part_agg = self.init_DFC.database.raw_collection.aggregate([{
                                                '$match': {
                                                    'url_name': re.compile(rf"^{name}_")
                                                }
                                            }])
                lists = list(part_agg)
                urls = [(part["url_name"], f"https://api.warframe.market/v1/items/{part['url_name']}/dropsources?include=item")
                    for part in lists]
                results = await self.init_DFC.fetch_all(urls)

                price_info = [(part["price_90d"], part["price_48h"]) for part in lists] 

                for (name, _), result, (p_90, p_48) in zip(urls, results, price_info):
                    relic_sources_info = [(relic_id, relic_data["rarity"]) 
                        for relic_data in result["payload"]["dropsources"] 
                        for relic_id in relic_data["relic"].split(",")]

                    ppn_sources_and_rarity = [
                    (name_.split(",")[0], part_rarity)
                    for ppn_source, part_rarity in relic_sources_info
                    for name_, id_ in self.init_DFC.filtered_data
                        if id_ in ppn_source]
                    
                    parts_in_set_list = result["include"]["item"]["items_in_set"]

                    for part in parts_in_set_list:
                        if name == part["url_name"]:
                            if self.init_DFC.not_set_pattern.match(name):
                                temp_list.append({
                                    "item_url": name,
                                    "ducats": part["ducats"],
                                    "item_id": part["id"],
                                    "trading_tax": part["trading_tax"],
                                    "quantity_for_set": part["quantity_for_set"],
                                    "item_name": part["en"]["item_name"],
                                    "price": {"price_90": p_90, "price_48": p_48},
                                    "ppn_source_and_rarity": ppn_sources_and_rarity})      
                                       
                    if self.init_DFC.set_pattern.match(name):
                        temp_data = {
                                "set_url": name,
                                "ducats": part["ducats"],
                                "set_id": part["id"],
                                "trading_tax": part["trading_tax"],
                                "price_set": {"set_p90d": p_90,
                                            "set_p48h": p_48},
                                "parts_in_set": temp_list
                        }
                if temp_data:
                    bulk_ops.append(InsertOne({"set_id": part["id"], **temp_data}))
            if bulk_ops:
                self.init_DFC.database.prime_sets_collection.bulk_write(bulk_ops)

        self.init_DFC.ps_not_corrupted = True
        self.init_DFC.ps_not_missing = True

        if self.init_DFC.toggle_pu_ps:
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
            
            updated_ps = self.init_DFC.database.prime_sets_collection.aggregate(pipeline)
            bulk_operations = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": doc}
                ) for doc in updated_ps
            ]
            self.init_DFC.database.prime_sets_collection.bulk_write(bulk_operations)
                
            
    async def load_relics(self):
        if not self.init_DFC.rc_not_corrupted:
            filtered_raw = {(name, _id) for name, _id in self.init_DFC.filtered_data if self.init_DFC.relic_pattern.match(name)}

            if self.init_DFC.rc_not_missing:
                existing_relics = {relic["relic_url"]: relic for relic in self.init_DFC.database.relics_collection.find()}
                filtered_raw = {(name, _id) for name, _id in filtered_raw if name not in existing_relics}

            bulk_ops = []
            for relic_name, relic_id in filtered_raw:
                relic_doc = self.init_DFC.database.raw_collection.find_one({"item_id": relic_id})

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
                    for prime_set in self.init_DFC.database.prime_sets_collection.find()
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
                if new_data:
                    bulk_ops.append(InsertOne({"relic_id": relic_id, **new_data}))
            if bulk_ops:
                self.init_DFC.database.relics_collection.bulk_write(bulk_ops)

        self.init_DFC.rc_not_corrupted = True
        self.init_DFC.rc_not_missing = True    

        if self.init_DFC.toggle_pu_rc:
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
            updated_ps = self.init_DFC.database.relics_collection.aggregate(pipeline)
            bulk_operations = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": doc}
                ) for doc in updated_ps
            ]
            self.init_DFC.database.relics_collection.bulk_write(bulk_operations)