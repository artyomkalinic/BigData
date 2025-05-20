INSERT INTO supplier (name, contact, email, phone, address, city, country)
SELECT DISTINCT 
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data;

INSERT INTO customer (first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed)
SELECT DISTINCT 
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data;

INSERT INTO seller (first_name, last_name, email, country, postal_code)
SELECT DISTINCT 
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data;

INSERT INTO store (name, location, city, state, country, phone, email)
SELECT DISTINCT 
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data;

INSERT INTO product (
    name, category, brand, material, weight, color, size,
    description, rating, reviews, release_date, expiry_date,
    pet_category, supplier_id
)
SELECT DISTINCT
    md.product_name,
    md.product_category,
    md.product_brand,
    md.product_material,
    md.product_weight,
    md.product_color,
    md.product_size,
    md.product_description,
    md.product_rating,
    md.product_reviews,
    md.product_release_date,
    md.product_expiry_date,
    md.pet_category,
    s.supplier_id
FROM mock_data md
JOIN supplier s
  ON md.supplier_name = s.name
 AND md.supplier_email = s.email;

INSERT INTO sales (
    sale_date,
    customer_id,
    seller_id,
    product_id,
    store_id,
    quantity,
    total_price
)
SELECT 
    md.sale_date,
    c.customer_id,
    sl.seller_id,
    p.product_id,
    st.store_id,
    md.sale_quantity,
    md.sale_total_price
FROM mock_data md
JOIN customer c
  ON md.customer_first_name = c.first_name
 AND md.customer_last_name = c.last_name
 AND md.customer_email = c.email
JOIN seller sl
  ON md.seller_first_name = sl.first_name
 AND md.seller_last_name = sl.last_name
 AND md.seller_email = sl.email
JOIN product p
  ON md.product_name = p.name
 AND md.product_brand = p.brand
 AND md.product_category = p.category
 and md.product_material = p.material
 and md.product_weight = p.weight
 and md.product_color = p.color
 and md.product_size = p.size
JOIN store st
  ON md.store_name = st.name
 AND md.store_email = st.email;
