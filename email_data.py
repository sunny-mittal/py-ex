import re
import aiohttp
import asyncio
import sys
import functools
from json import dumps
from datetime import datetime

IMG_SIZE = "354x255"
TYPE = "property_image"


def get_uri(listing_id):
    """This would be replaced with your api, of course but this service is pretty rad and let me create a fake API with your real data"""
    return f'https://my-json-server.typicode.com/sunny-mittal/py-ex/listings/{listing_id}'


def extract_fields(json):
    """Get values of interest with defaults to prevent having to check existence"""
    content = json.get('content', [])
    pricing = json.get('pricing', {})
    branch_id = json.get('branch_id', 'unknown')
    listing_id = json.get('listing_id')
    address = json.get('display_address')

    price = pricing.get('price', 0)
    image_url = ''  # initialize image_url to empty string in case no `"type": "property_image"` exists

    for item in content:
        if item.get('type') == TYPE:
            image_url = item.get(IMG_SIZE, '')
            break

    return {'branch_id': branch_id, 'listing_id': listing_id, 'address': address, 'price': price, 'image_url': image_url}


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


def reduce_handler(data, listing):
    branches = data.get('branches')
    branch_id = listing.get('branch_id')
    branch_listings = branches.get(branch_id, [])
    branch_listings.append(listing)
    branches.update({branch_id: branch_listings})
    return data


def process_results(results):
    return functools.reduce(reduce_handler, results, {"branches": {}})


async def main():
    filename = sys.argv[1]
    listing_ids = get_listing_ids(filename)
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[fetch(session, id) for id in listing_ids])
        json = dumps(process_results(results), indent=2)
        date = datetime.strptime("20.12.2016 09:38:42,76",
                                 "%d.%m.%Y %H:%M:%S,%f").strftime('%s.%f')
        timestamp = int(float(date)*1000)
        f = open(f'email_data-{timestamp}.json', 'w')
        f.write(json)
        f.close()


if __name__ == '__main__':
    asyncio.run(main())
