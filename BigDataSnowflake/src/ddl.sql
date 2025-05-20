CREATE TABLE mock_data (
    id INT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(10,2),
    product_quantity INT,
    sale_date DATE,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price NUMERIC(10,2),
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight NUMERIC(10,2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE supplier (
    supplier_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE product (
    product_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    brand TEXT,
    material TEXT,
    weight NUMERIC(10, 2),
    color TEXT,
    size TEXT,
    description TEXT,
    rating NUMERIC(3, 1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    pet_category TEXT,
    supplier_id INTEGER REFERENCES supplier(supplier_id)
);

CREATE TABLE customer (
    customer_id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE seller (
    seller_id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE store (
    store_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    customer_id INTEGER REFERENCES customer(customer_id),
    seller_id INTEGER REFERENCES seller(seller_id),
    product_id INTEGER REFERENCES product(product_id),
    store_id INTEGER REFERENCES store(store_id),
    quantity INTEGER,
    total_price NUMERIC(10, 2)
);
