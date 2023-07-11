import asyncio
import datetime
import aiohttp
from models import Base, Session, SwApi_people, engine
from more_itertools import chunked

MAX_CH_SIZE = 10


async def find_names(json_data, field, name):
    session = aiohttp.ClientSession()
    if len(json_data[field]) > 0:
        values_list = []
        for el in json_data[field]:
            response = await session.get(el)
            json_d = await response.json()
            value = json_d[name]
            values_list.append(value)
        json_data[field] = ",".join(values_list)
    else:
        json_data[field] = ""
    await session.close()


async def correct_json(json_data):
    json_data.pop("created", None)
    json_data.pop("edited", None)
    json_data.pop("url", None)
    coro_1 = find_names(json_data, field="films", name="title")
    coro_2 = find_names(json_data, field="vehicles", name="name")
    coro_3 = find_names(json_data, field="starships", name="name")
    coro_4 = find_names(json_data, field="species", name="name")
    await asyncio.gather(coro_1, coro_2, coro_3, coro_4)
    return json_data


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    st_code = response.status
    if st_code == 200:
        json_data = await response.json()
        json_data = await correct_json(json_data)
        await session.close()
        return json_data
    else:
        await session.close()


async def insert_into_db(people_json_list):
    async with Session() as session:
        sw_json_list = [SwApi_people(**people_json) for people_json in people_json_list]
        session.add_all(sw_json_list)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    for ids_chunk in chunked(range(1, 91), MAX_CH_SIZE):
        get_people_coros = [get_people(people_id) for people_id in ids_chunk]
        people_json_list = await asyncio.gather(*get_people_coros)
        while None in people_json_list:
            people_json_list.remove(None)
        asyncio.create_task(insert_into_db(people_json_list))

    current_task = asyncio.current_task()
    tasks_sets = asyncio.all_tasks()
    tasks_sets.remove(current_task)

    await asyncio.gather(*tasks_sets)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
