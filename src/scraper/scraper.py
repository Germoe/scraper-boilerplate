import requests as req
from pathlib import Path
import pandas as pd

def scrape_func(zip_code, path, proxies, timeout, radius=100):
    '''
        Add your custom Scrape function here. As an example you can find the scrape function to get Walmart Stores across the US.
        This example will scrape all Walmarts (does not include Sam's Club)
    '''
    stores = []
    this = Path(path)
    if this.is_file():
        # Zip Code exists
        return False
    # Make sure the headers are still correct
    url="https://www.walmart.com/store/finder/electrode/api/stores?singleLineAddr={}&distance={}".format(zip_code, radius)
    headers={ 'accept':'*/*',
            'accept-encoding':'gzip, deflate, br',
            'accept-language':'en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7',
            'cache-control':'max-age=0, no-cache, no-store',
            'upgrade-insecure-requests':'1',
            'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36'
    }
    get_store = req.get(url, 
        headers=headers, 
        proxies=proxies, 
        verify=False,
        timeout=timeout
    )
    store_response = get_store.json()
    stores_data = store_response.get('payload',{}).get("storesData",{}).get("stores",[])
    if not stores_data:
        print('no stores found near %s'%(zip_code))
        stores = [{
                'name':'',
                'distance':'',
                'address':'',
                'zip_code':'',
                'city':'',
                'store_id':'',
                'phone':'',
        }]
    else:
        print('processing store details')
        #iterating through all stores
        for store in stores_data:
            store_id = store.get('id')
            display_name = store.get('displayName')
            address = store.get('address').get('address')
            postal_code = store.get('address').get('postalCode')
            city = store.get('address').get('city')
            phone = store.get('phone')
            distance = store.get('distance')

            data = {
                    'name':display_name,
                    'distance':distance,
                    'address':address,
                    'zip_code':postal_code,
                    'city':city,
                    'store_id':store_id,
                    'phone':phone,
            }
            stores.append(data)
    stores = pd.DataFrame(stores)
    stores.to_csv(path, sep='\t', encoding='utf-8')
    return True