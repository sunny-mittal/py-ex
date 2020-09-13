import re
import aiohttp
import asyncio
import sys
import functools


def get_uri(listing_id):
    """This would be replaced with your api, of course"""
    return f'https://jsonplaceholder.typicode.com/todos/{listing_id}'


def extract_fields(json):
    """You can use a similar approach for your stuff"""
    return json


async def fetch(session, listing_id):
    uri = get_uri(listing_id)
    response = await session.get(uri)
    json = await response.json()
    return extract_fields(json)


def get_listing_ids(file):
    f = open(file, 'r')
    listing_ids = []
    for line in f:
        listing_ids.extend(re.split(r'\D+', line))
    f.close()
    return listing_ids


def process_results(results):
    print(results)
    # functools.reduce(results)


async def main():
    filename = sys.argv[1]
    listing_ids = get_listing_ids(filename)
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[fetch(session, id) for id in listing_ids])
        process_results(results)


if __name__ == '__main__':
    asyncio.run(main())
