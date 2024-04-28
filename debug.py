#WTB [Aya] relics 6:25p! WTT [Torid Argi-Satitis] for another riven of equal!
#WTB [Aya] (6:25p, rad 6:35p) WTS [Glaive Acri-Deciata] [Glaive Acri-Exicron] [Glaive Crita-Tempitis] ~900 [Glaive Croni-Visitis] [Glaive Croni-Acricron] 1k2 ~ 1k8

import cProfile
import pstats
import asyncio
import aiohttp
import test_data_str

async def main():

    test = test_data_str.Data_Fetch_Conditions()
    test_load = test_data_str.Database_Upload()
    await test.async_init(aiohttp.ClientSession())
    await test_load.instantiate_DFC() 

    # test.set_price_updated(False)
    # test.toggle_price_ps(True)
    # test.toggle_price_rc(True)

    print(f"Before: Test raw not corrupted: {test.rd_not_corrupted}")
    print(f"Before: Test raw not missing: {test.rd_not_missing}")

    print(f"Before: Test ps not corrupted: {test.ps_not_corrupted}")
    print(f"Before: Test ps not missing: {test.ps_not_missing}")
    print(f"Before: Test ps needs price update: {test.toggle_pu_ps}")   

    print(f"Before: Test rc not corrupted: {test.rc_not_corrupted}")
    print(f"Before: Test rc not missing: {test.rc_not_missing}")
    print(f"Before: Test rc needs price update: {test.toggle_pu_rc}")

    print(f"Before: Test price fully updated: {test.price_updated}")
 
    await test_load.load_raw_and_price()
    await test_load.load_prime_sets()
    await test_load.load_relics()
    await test.close()
    await test.close()
    await test.close()


    print(f"After: Test raw not corrupted: {test_load.init_DFC.rd_not_corrupted}")
    print(f"After: Test raw not missing: {test_load.init_DFC.rd_not_missing}")

    print(f"After: Test ps not corrupted: {test_load.init_DFC.ps_not_corrupted}")
    print(f"After: Test ps not missing: {test_load.init_DFC.ps_not_missing}")

    print(f"After: Test rc not corrupted: {test_load.init_DFC.rc_not_corrupted}")
    print(f"After: Test rc not missing: {test_load.init_DFC.rc_not_missing}")

    print(f"After: Test price fully updated: {test_load.init_DFC.price_updated}")

profiler = cProfile.Profile()
profiler.enable()
asyncio.run(main())
profiler.disable()

stats = pstats.Stats(profiler)
stats.sort_stats('cumulative').print_stats(30)