import gzip
import hashlib
import json
import os
import re
import requests
import pymysql
from lxml import html
from db_maker import nobero_products
import time
from sys import argv

start_time = time.time()

start = argv[1]
end = argv[2]


def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Directory {path} created")


class Scraper:
    def __init__(self):
        self.session = requests.Session()  # Initialize session in the Scraper class
        self.client = pymysql.connect(
            host='localhost',
            user='root',
            database='nobero_db',
            password='actowiz',
            charset='utf8mb4',
            autocommit=True
        )
        if self.client.open:
            print('Database connection Successful!')
        else:
            print('Database connection Un-Successful.')
        self.cursor = self.client.cursor()

        # Creating Saved Pages Directory for this Project if not Exists
        project_name = 'Nobero'

        self.project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
        ensure_directory_exists(path=self.project_files_dir)

    def req_sender(self, url: str, method: str, headers=None, data=None, params=None):
        _response = self.session.request(method=method, url=url, params=params, headers=headers, data=data)
        if _response.status_code != 200:
            print(f"HTTP Request Status code: {_response.status_code}")  # HTTP response error
            return None
        else:
            return _response  # Request Successful

    def page_checker(self, url: str, method: str, path: str, headers=None, data=None, params=None) -> str:
        page_hash = hashlib.sha256(url.encode()).hexdigest()
        file_path = os.path.join(path, f"{page_hash}.html.gz")
        ensure_directory_exists(path)
        if os.path.exists(file_path):
            print("Page exists, Reading it...")
            print(f'File name is : {page_hash}')
            with gzip.open(file_path, mode='rb') as file:
                html_text = file.read().decode(errors='backslashreplace')
            return html_text
        else:
            _response = self.req_sender(url=url, method=method, headers=headers, data=data, params=params)
            print(_response.status_code)
            if _response is not None:
                parsed_next_page_html = html.fromstring(_response.text)
                xpath_products_data = '//section[@class="product-card-container h-full"]'
                products_data = parsed_next_page_html.xpath(xpath_products_data)
                if products_data:
                    print('prod data')
                    print("Page does not exist, Sending Request...")
                    print(f'File name is : {page_hash}')
                    with gzip.open(file_path, mode='wb') as file:
                        file.write(_response.content)
                        print("Page Saved")
                    return _response.text
                elif not products_data:
                    print('no prod data')
                    print("Page does not exist, Sending Request...")
                    print(f'File name is : {page_hash}')
                    with gzip.open(file_path, mode='wb') as file:
                        file.write(_response.content)
                        print("Page Saved")
                    return _response.text
                else:
                    return 'No Products'

    def db_schema_creater(self):
        self.cursor.execute(nobero_products)
        print('PINCODES Table created!')

    def db_saver(self):
        # To create tables if not exists
        self.db_schema_creater()

        # Fetching each Product link from database to send request
        select_query = f'''SELECT * FROM nobero_products_status
                            WHERE product_status = 'pending' and id between {start} and {end};'''
        self.cursor.execute(select_query)
        products_data = self.cursor.fetchall()
        for this_product_data in products_data:
            this_product_id = this_product_data[0]
            this_product_url = this_product_data[1]
            this_product_category_url = this_product_data[2]
            print('Category url: ', this_product_category_url)
            print('Product url: ', this_product_url)

            #  Sending request on each product to scrape its data
            this_product_directory = os.path.join(self.project_files_dir, 'Category_Pages', f'{this_product_category_url.split('/')[-1]}', 'product_data')
            product_html_response = self.page_checker(url=this_product_url, method='GET', path=this_product_directory)

            parsed_product_html = html.fromstring(product_html_response)

            product_discount = int(parsed_product_html.xpath('//h2[contains(@class,"discount-flat")]/text()')[0].split(' ')[0].split('₹')[1])
            product_bought_status_ = parsed_product_html.xpath("//span[contains(text(), 'people bought this in last')]/text()")
            product_bought_status = ' '.join(product_bought_status_[0].strip().split()) if product_bought_status_ else 'N/A'
            product_offers_list = parsed_product_html.xpath('//div[contains(@class, "buy-n-wrapper")]//div[@class="flex flex-col"]/span/text()')
            product_offer_1 = ' '.join(product_offers_list[:2]) if product_offers_list[:2] else 'N/A'
            product_offer_2 = ' '.join(product_offers_list[2:4]) if product_offers_list[2:4] else 'N/A'
            product_offer_3 = ' '.join(product_offers_list[2:4]) if product_offers_list[4:6] else 'N/A'
            product_offers_dict = dict()
            product_offers_dict['offer_1'] = product_offer_1
            product_offers_dict['offer_2'] = product_offer_2
            product_offers_dict['offer_3'] = product_offer_3
            product_offers_dict = json.dumps(product_offers_dict)

            pattern = re.compile(r'\s+')
            product_description_spaced = ' '.join(parsed_product_html.xpath('//div[@id="description_content"]//text()'))
            product_description = re.sub(pattern=pattern, repl=' ', string=product_description_spaced).strip()
            product_free_shipping_spaced = ' '.join(parsed_product_html.xpath('//div[@id="free_shipping_content"]//text()'))
            product_free_shipping = re.sub(pattern=pattern, repl=' ', string=product_free_shipping_spaced).strip()
            product_return_spaced = ' '.join(parsed_product_html.xpath('//div[@id="return_content"]//text()'))
            product_return = re.sub(pattern=pattern, repl=' ', string=product_return_spaced).strip()

            product_sale_countdown = ' '.join(parsed_product_html.xpath('//div[@id="sales_countdown"]/text()')[0].split())
            # If you want to store time in TIME format for TIME datatype, uncomment below 2 lines and change datatype in DB table
            # time_string = product_sale_countdown.replace("h", "").replace("m", "").replace("s", "").replace(" ", "")
            # product_sale_countdown = datetime.strptime(time_string, "%H:%M:%S").time()
            key_feat_elm = parsed_product_html.xpath('//div[@class="product-metafields-values text-sm lg:text-base"]')
            key_feat_dict = dict()
            for key_feat in key_feat_elm:
                feature = key_feat.xpath('./h4/text()')[0]
                value = key_feat.xpath('./p/text()')[0]
                key_feat_dict[feature] = value
            product_key_highlights = json.dumps(key_feat_dict)

            xpath_script_tag = '//script[contains(text(), "variant_ids")]/text()'
            script_tag = parsed_product_html.xpath(xpath_script_tag)[1]
            script_tag_dict_list = json.loads(script_tag)  # Converting Json data into Dictionary
            print(type(script_tag_dict_list))
            # Iterating into each variant of product to store their data into DB
            count = 1
            for product_dict in script_tag_dict_list:
                this_product_dict = dict()
                print(product_dict)
                # Data from dictionary
                this_product_dict['product_id'] = product_dict.get('id')
                this_product_dict['product_name'] = product_dict.get('name')
                this_product_dict['product_link'] = this_product_url + f'?{product_dict.get('id')}'
                this_product_dict['product_color'] = product_dict.get('options')[0]
                this_product_dict['product_size'] = product_dict.get('options')[1]
                image_link = product_dict.get('featured_image').get('src').lstrip('/') if product_dict.get('featured_image') != 'None' else 'N/A'
                this_product_dict['product_image_link'] = image_link
                this_product_dict['product_availability'] = product_dict.get('available')
                this_product_dict['product_price'] = product_dict.get('price') // 100
                this_product_dict['product_mrp'] = product_dict.get('compare_at_price') // 100
                # Data from page using Xpath
                this_product_dict['product_discount'] = product_discount
                this_product_dict['product_bought_status'] = product_bought_status
                this_product_dict['product_offers'] = product_offers_dict
                this_product_dict['product_sale_countdown'] = product_sale_countdown
                this_product_dict['product_key_highlights'] = product_key_highlights
                this_product_dict['product_description'] = product_description
                this_product_dict['product_free_shipping'] = product_free_shipping
                this_product_dict['product_return'] = product_return

                # Storing into Database
                print('Storing into Database')
                cols = this_product_dict.keys()
                rows = this_product_dict.values()
                insert_query = f'''INSERT INTO `nobero_products` ({', '.join(tuple(cols))}) VALUES ({('%s, ' * len(this_product_dict)).rstrip(", ")});'''
                try:
                    self.cursor.execute(query=insert_query, args=tuple(rows))
                except Exception as e:
                    print(e)

                print(f'{count}nt Product {this_product_dict['product_link']} of {this_product_url} from Category {this_product_category_url} Done')
                count += 1
                print('+' * 15)
            print('-' * 30)
            # Updating link status to DONE
            print(f'{this_product_url} Done.')
            update_query = f'''UPDATE nobero_products_status
                               SET product_status = 'Done'
                               WHERE id = '{this_product_id}';'''
            self.cursor.execute(update_query)


Scraper().db_saver()
