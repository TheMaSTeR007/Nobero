# Creating pincodes table if not exists
nobero_links_create_query = f'''CREATE TABLE IF NOT EXISTS nobero_links (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                page_link VARCHAR(255) unique,
                                link_status VARCHAR(255) DEFAULT 'Pending'
                                );'''

nobero_products_status_create_query = f'''CREATE TABLE IF NOT EXISTS nobero_products_status (
                                            id INT AUTO_INCREMENT PRIMARY KEY,
                                            product_link VARCHAR(255) UNIQUE,
                                            page_link VARCHAR(255),
                                            product_status VARCHAR(255) DEFAULT 'pending'
                                            );'''

nobero_products = f'''CREATE TABLE IF NOT EXISTS nobero_products (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        product_id VARCHAR(255),
                        product_name VARCHAR(255),
                        product_link VARCHAR(255),
                        product_color VARCHAR(255),
                        product_size VARCHAR(255),
                        product_image_link VARCHAR(255),
                        product_availability BOOL,
                        product_price INT,
                        product_mrp INT,
                        product_discount INT,
                        product_bought_status VARCHAR(255),
                        product_offers JSON,
                        product_sale_countdown VARCHAR(255),
                        product_key_highlights JSON,
                        product_description TEXT,
                        product_free_shipping TEXT,
                        product_return TEXT
                        );'''
