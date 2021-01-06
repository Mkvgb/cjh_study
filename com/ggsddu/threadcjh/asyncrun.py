from db.pgsql import PGConnector
import asyncio
import asyncpg
import time


async def task_1():
    print("task 1 start")
    start = time.time()
    conn = await asyncpg.connect(user='postgres', password='postgres',
                                 database='police_analysis_db', host='192.168.1.99',
                                 port=6543)
    values = await conn.fetch('''select * from audit_data.gd_ele_fence limit 200000''')
    print("task 1 end")
    # print("task 1 result", values)
    print("task 1 result length", len(values))
    end = time.time()
    print("task 1 time spend", int(end-start))
    await conn.close()


async def task_2():
    print("task 2 start")
    start = time.time()
    conn = await asyncpg.connect(user='postgres', password='postgres',
                                 database='police_analysis_db', host='192.168.1.99',
                                 port=6543)
    values = await conn.fetch('''select * from audit_data.gd_ele_fence limit 200000 offset 100''')
    print("task 2 end")
    # print("task 2 result", values)
    print("task 2 result length", len(values))
    end = time.time()
    print("task 2 time spend", int(end-start))
    await conn.close()


async def task_3():
    print("task 3 start")
    start = time.time()
    conn = await asyncpg.connect(user='postgres', password='postgres',
                                 database='police_analysis_db', host='192.168.1.99',
                                 port=6543)
    values = await conn.fetch('''select * from audit_data.gd_ele_fence limit 300000 offset 200''')
    print("task 3 end")
    # print("task 3 result", values)
    print("task 3 result length", len(values))
    end = time.time()
    print("task 3 time spend", int(end-start))
    await conn.close()


async def task_4():
    print("task 4 start")
    start = time.time()
    conn = await asyncpg.connect(user='postgres', password='postgres',
                                 database='police_analysis_db', host='192.168.1.99',
                                 port=6543)
    values = await conn.fetch('''select * from audit_data.gd_ele_fence limit 300000 offset 200''')
    print("task 4 end")
    # print("task 4 result", values)
    print("task 4 result length", len(values))
    end = time.time()
    print("task 4 time spend", int(end-start))
    await conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    input_coroutines = [task_1(), task_2(), task_3(), task_4()]
    loop.run_until_complete(asyncio.gather(*input_coroutines, return_exceptions=True))
    loop.close()
