import gzip
import hashlib
import os
import requests
import pymysql
from lxml import html
from db_maker import nobero_links_create_query, nobero_products_status_create_query
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
                    return 'No Products'
                else:
                    print('no prod data')
                    print("Page does not exist, Sending Request...")
                    print(f'File name is : {page_hash}')
                    with gzip.open(file_path, mode='wb') as file:
                        file.write(_response.content)
                        print("Page Saved")
                    return _response.text

    def db_schema_creater(self):
        self.cursor.execute(nobero_links_create_query)
        print('Nobero Links Table created!')
        self.cursor.execute(nobero_products_status_create_query)
        print('Nobero Products Status Table created!')
        # self.cursor.execute(new_bc_registry_create_query)
        # print('PINCODES Table created!')

    def scrape(self):
        # To create tables if not exists
        self.db_schema_creater()

        url = 'https://nobero.com/'
        response = self.req_sender(url=url, method='GET', headers=None, params=None, data=None)
        html_text = response.text
        parsed_html = html.fromstring(html_text)
        links_list = parsed_html.xpath('//div[@class="collect-contain py-8"]/a/@href')
        # writing a lambda function with map to make relative urls absolute if they are present
        page_links_list = list(map(lambda page_url: 'https://nobero.com' + page_url if not page_url.startswith('https://nobero.com/') else page_url, links_list))

        # storing all links into database for further process
        for page_link in page_links_list:
            try:
                insert_query = f'''INSERT INTO nobero_links (page_link) VAlUES (%s);'''
                self.cursor.execute(insert_query, args=(page_link,))
                print(f'Inserting {page_link} into nobero_links table.')
            except Exception as e:
                print(e)

        # Fetching each page link from database to send request
        select_query = f'''SELECT * FROM nobero_links WHERE link_status = 'pending';'''
        self.cursor.execute(select_query)
        category_links_data = self.cursor.fetchall()
        print('Category links: ', category_links_data)

        # Sending requests on each category_url to get products data
        for this_category_data in category_links_data:
            this_category_id = this_category_data[0]
            this_category_url = this_category_data[1]
            print(this_category_url)

            page_count = 1
            this_page_url = f'{this_category_url}?page={page_count}'
            html_response = self.page_checker(url=this_page_url, method='GET', path=os.path.join(self.project_files_dir, 'Category_Pages', f'{this_category_url.split('/')[-1]}'))
            while html_response != 'No Products':  # Here we skip the page that does not consist of any products
                parsed_html = html.fromstring(html_response)
                xpath_product_link = '//section[@class="product-card-container h-full"]//a/@href'
                products_links = parsed_html.xpath(xpath_product_link)

                # Here looping over each Product and storing their data in database
                for each_product_relative_link in products_links:
                    this_product_link = 'https://nobero.com' + each_product_relative_link
                    product_insert_query = f'''INSERT INTO nobero_products_status (product_link, page_link) VALUES (%s, %s);'''
                    try:
                        self.cursor.execute(product_insert_query, args=(this_product_link, this_page_url))
                    except Exception as e:
                        print(e)

                #  Sending request on next page if it has products in it
                page_count += 1
                this_page_url = f'{this_category_url}?page={page_count}'
                print(this_page_url)
                html_response = self.page_checker(url=this_page_url, method='GET', path=os.path.join(self.project_files_dir, 'Category_Pages', f'{this_category_url.split('/')[-1]}'))
            print(f'No Proucts on {this_page_url}')

            # Updating link status to DONE
            print(f'{this_category_url} Done.')
            update_query = f'''UPDATE nobero_links
                               SET link_status = 'Done'
                               WHERE id = '{this_category_id}';'''
            self.cursor.execute(update_query)

Scraper().scrape()

