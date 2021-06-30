COPY_SQL = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{{}}'
SECRET_ACCESS_KEY '{{}}'
IGNOREHEADER 1
DELIMITER ';'
"""

COPY_ALL_RIDES_SQL = COPY_SQL.format(
    "staging_rides",
    f's3://uber-tracking-expenses-bucket-s3-{{}}/rides/rides_receipts.csv'
)

COPY_ALL_EATS_SQL = COPY_SQL.format(
    "staging_eats",
    f's3://uber-tracking-expenses-bucket-s3-{{}}/eats/eats_receipts.csv'
)

COPY_ALL_EATS_ITEMS_SQL = COPY_SQL.format(
    "staging_eats_items",
    f's3://uber-tracking-expenses-bucket-s3-{{}}/eats/items_eats_receipts.csv'
)


create_staging_eats = ("""
    DROP TABLE IF EXISTS staging_eats;
    CREATE TABLE staging_eats(
        event_id INT IDENTITY(0,1),
        subject VARCHAR(255),
        userservice VARCHAR(255),
        uber_email VARCHAR(255),
        date TIMESTAMP,
        filename VARCHAR(255),
        service VARCHAR(50),
        amount_charged DOUBLE PRECISION,
        total DOUBLE PRECISION,
        subtotal DOUBLE PRECISION,
        delivery_fee DOUBLE PRECISION,
        service_Fee DOUBLE PRECISION,
        change DOUBLE PRECISION,
        restaurant VARCHAR(255),
        picked_up_from VARCHAR(255),
        delivered_to VARCHAR(255),
        lat_from DECIMAL(10, 8),
        long_from DECIMAL(11, 8),
        lat_to DECIMAL(10, 8),
        long_to DECIMAL(11, 8),
        items INTEGER);       
""")

create_staging_rides = ("""
    DROP TABLE IF EXISTS staging_rides;
    CREATE TABLE staging_rides(
        event_id INT IDENTITY(0,1),
        subject VARCHAR(255),
        userservice VARCHAR(255),
        uber_email VARCHAR(255),
        date TIMESTAMP,
        filename VARCHAR(255),
        service VARCHAR(50),
        amount_charged DOUBLE PRECISION,
        total DOUBLE PRECISION,
        subtotal DOUBLE PRECISION,
        booking_fee DOUBLE PRECISION,
        government_contribution DOUBLE PRECISION,
        wait_time DOUBLE PRECISION,
        trip_fare DOUBLE PRECISION,
        discounts DOUBLE PRECISION,
        before_Taxes DOUBLE PRECISION,
        balance DOUBLE PRECISION,
        time_payment DOUBLE PRECISION,
        distance DOUBLE PRECISION,
        unsettled_past_uber_trip DOUBLE PRECISION,
        distance_service DOUBLE PRECISION,
        time_from_service TIMESTAMP,
        time_to_service TIMESTAMP,
        from_address VARCHAR(255),
        to_address VARCHAR(255),
        lat_from DECIMAL(10, 8),
        long_from DECIMAL(11, 8),
        lat_to DECIMAL(10, 8),
        long_to DECIMAL(11, 8));            
""")

create_staging_eats_items  = ("""
    DROP TABLE IF EXISTS staging_eats_items;
    CREATE TABLE staging_eats_items(
        event_id INT IDENTITY(0,1),
        item VARCHAR(255),
        qty INTEGER,
        cost DOUBLE PRECISION,
        id INTEGER);   
""")

create_fact_rides  = ("""
    DROP TABLE IF EXISTS fact_rides;
    CREATE TABLE fact_rides(
        id_ride INTEGER NOT NULL PRIMARY KEY,
        id_date timestamp NOT NULL,
        id_user INTEGER NOT NULL,
        amount_charged DOUBLE PRECISION NOT NULL,
        total DOUBLE PRECISION NOT NULL,
        subtotal DOUBLE PRECISION NOT NULL,
        booking_fee DOUBLE PRECISION NOT NULL,
        government_contribution DOUBLE PRECISION NOT NULL,
        wait_time DOUBLE PRECISION,
        trip_Fare DOUBLE PRECISION,
        discounts DOUBLE PRECISION,
        before_Taxes DOUBLE PRECISION,
        balance DOUBLE PRECISION,
        time_payment DOUBLE PRECISION,
        distance_payment DOUBLE PRECISION,
        unsettled_past_uber_trip DOUBLE PRECISION,
        distance_service DOUBLE PRECISION NOT NULL,
        time_service DOUBLE PRECISION NOT NULL,
        id_time_from_service TIMESTAMP NOT NULL,
        id_time_to_service TIMESTAMP NOT NULL,
        id_from_location INTEGER NOT NULL, 
        id_to_location INTEGER NOT NULL)
        DISTSTYLE AUTO
        SORTKEY(id_date, id_ride);        
""")

create_fact_eats  = ("""
    DROP TABLE IF EXISTS fact_eats;
    CREATE TABLE fact_eats(
        id_order INTEGER NOT NULL PRIMARY KEY,
        id_date timestamp NOT NULL,
        id_user INTEGER NOT NULL,
        amount_charged DOUBLE PRECISION NOT NULL,
        total DOUBLE PRECISION NOT NULL,
        subtotal DOUBLE PRECISION NOT NULL,
        delivery_fee DOUBLE PRECISION NOT NULL,
        service_Fee DOUBLE PRECISION NOT NULL,
        change DOUBLE PRECISION NOT NULL,
        id_restaurant INTEGER NOT NULL,
        id_delivered_to_location INTEGER NOT NULL)
        DISTSTYLE AUTO
        SORTKEY(id_date, id_order);        
""")

create_dim_products  = ("""
    DROP TABLE IF EXISTS dim_products;
    CREATE TABLE dim_products(
        id INTEGER NOT NULL PRIMARY KEY identity(1,1),
        product_name NVARCHAR(255));  
""")

create_dim_products_order  = ("""
    DROP TABLE IF EXISTS dim_products_order;
    CREATE TABLE dim_products_order(
        id_product_order INTEGER NOT NULL PRIMARY KEY identity(1,1),
        id_order INTEGER NOT NULL,
        id_product INTEGER NOT NULL,
        qty INTEGER NOT NULL,
        cost DOUBLE PRECISION NOT NULL);    
""")

create_dim_restaurants  = ("""
    DROP TABLE IF EXISTS dim_restaurants;
    CREATE TABLE dim_restaurants(
        id INTEGER NOT NULL PRIMARY KEY identity(1,1),
        name NVARCHAR(255) NOT NULL,
        id_location INTEGER NOT NULL);
""")

create_dim_users  = ("""
    DROP TABLE IF EXISTS dim_users;
    CREATE TABLE dim_users(
        id INTEGER NOT NULL PRIMARY KEY identity(1,1),
        email NVARCHAR(1000) NOT NULL);
""")

create_dim_locations  = ("""
    DROP TABLE IF EXISTS dim_locations;
    CREATE TABLE dim_locations(
        id INTEGER NOT NULL PRIMARY KEY identity(1,1),
        lat DECIMAL(10, 8) NOT NULL,
        long DECIMAL(11, 8) NOT NULL,
        address NVARCHAR(1000) NOT NULL);
""")

create_dim_times = ("""
    DROP TABLE IF EXISTS dim_times;
    CREATE TABLE dim_times(
        date TIMESTAMP not null distkey sortkey,
        hour INTEGER, 
        day INTEGER,
        week INTEGER, 
        month INTEGER,
        year INTEGER, 
        weekday INTEGER,
        is_weekend boolean,
        PRIMARY KEY(date));
""")

create_dim_weekday = ("""
    DROP TABLE IF EXISTS dim_weekday;
    CREATE TABLE dim_weekday(
        id  INTEGER not null PRIMARY KEY,
        weekday_name NVARCHAR(20));
""")

create_dim_month = ("""
    DROP TABLE IF EXISTS dim_month;
    CREATE TABLE dim_month(
        id  INTEGER not null PRIMARY KEY,
        month_name NVARCHAR(20));
""")

create_dim_year = ("""
    DROP TABLE IF EXISTS dim_year;
    CREATE TABLE dim_year(
        id  INTEGER not null PRIMARY KEY);
""")

create_dim_hour = ("""
    DROP TABLE IF EXISTS dim_hour;
    CREATE TABLE dim_hour(
        id  INTEGER not null PRIMARY KEY); 
""")

load_dim_times = ("""
    INSERT INTO dim_times(date, hour, day, week, month, year, weekday, is_weekend)
    (SELECT date,
        extract(hour from date) as hour,
        extract(day from date) as day ,
        extract(week from date) as week, 
        extract(month from date) as month,
        extract(year from date) as year, 
        extract(weekday from date) as weekday,
        decode(date_part(dow,date),0,true,6,true,false) as is_weekend
    FROM staging_eats
    UNION
    SELECT date,
        extract(hour from date) as hour,
        extract(day from date) as day ,
        extract(week from date) as week, 
        extract(month from date) as month,
        extract(year from date) as year, 
        extract(weekday from date) as weekday,
        decode(date_part(dow,date),0,true,6,true,false) as is_weekend
    FROM staging_rides
    UNION
    SELECT time_from_service,
        extract(hour from time_from_service) as hour,
        extract(day from time_from_service) as day ,
        extract(week from time_from_service) as week, 
        extract(month from time_from_service) as month,
        extract(year from time_from_service) as year, 
        extract(weekday from time_from_service) as weekday,
        decode(date_part(dow,time_from_service),0,true,6,true,false) as is_weekend
    FROM staging_rides
    UNION
    SELECT time_to_service,
        extract(hour from time_to_service) as hour,
        extract(day from time_to_service) as day ,
        extract(week from time_to_service) as week, 
        extract(month from time_to_service) as month,
        extract(year from time_to_service) as year, 
        extract(weekday from time_to_service) as weekday,
        decode(date_part(dow,time_to_service),0,true,6,true,false) as is_weekend
    FROM staging_rides);
""")

load_dim_hour = ("""
    INSERT INTO dim_hour(id) values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(0);
""")

load_dim_month = ("""
    INSERT INTO dim_month values(1, 'January'),
    (2, 'February'),
    (3, 'March'),
    (4, 'April'),
    (5, 'May'),
    (6, 'June'),
    (7, 'July'),
    (8, 'August'),
    (9, 'September'),
    (10, 'October'),
    (11, 'November'),
    (12, 'December');
""")

load_dim_weekday = ("""
    INSERT INTO dim_weekday values(1, 'Monday'),
    (2, 'Tuesday'),
    (3, 'Wednesday'),
    (4, 'Thursday'),
    (5, 'Friday'),
    (6, 'Saturday'),
    (0, 'Sunday');
""")

load_dim_year = ("""
    INSERT INTO dim_year(id) values(2013), (2014), (2015), (2016), (2017), (2018), (2019), (2020), (2021);
""")

load_dim_products = ("""
    insert into dim_products(product_name)(select distinct item from staging_eats_items);
""")

load_dim_products_order = ("""
    insert into dim_products_order(id_order, id_product, qty,  cost)
    (SELECT se.event_id as id_order, 
        p.id as id_product,
        sei.qty, 
        sei.cost
    FROM staging_eats_items sei,
        staging_eats se,
        dim_products p
    WHERE p.product_name = sei.item and
        sei.id = se.items);
""")

load_dim_locations = ("""
    INSERT INTO dim_locations(address, lat, long)
    (
    select distinct delivered_to as address,  0.0 as lat, 0.0 as long
    FROM staging_eats
    UNION
    select distinct picked_up_from as address, 0.0 as lat, 0.0 as long
    from staging_eats
    UNION
    select distinct from_address as address, 0.0 as lat, 0.0 as long
    from staging_rides
    UNION
    select distinct to_address as address, 0.0 as lat, 0.0 as long
    from staging_rides
    );
""")

load_dim_restaurants = ("""
    insert into dim_restaurants(name, id_location)
    (select distinct restaurant as name,
        (select id from dim_locations where address = picked_up_from) as id_location
    from staging_eats);
""")

load_dim_users = ("""
    insert into dim_users(email)
    (select distinct userservice 
    from staging_eats
    union
    select distinct userservice
    from staging_rides)
""")


load_fact_eats = ("""
    INSERT INTO fact_eats
    (SELECT EVENT_ID as id_order,
        date as id_date,
        (select id from dim_users where email = userservice) as id_user,
        amount_charged,
        total,
        subtotal,
        delivery_fee,
        nvl2(service_Fee, service_Fee, 0.0) as service_Fee,
        nvl2(change, change, 0.0) as change,
        (select id from dim_restaurants where name = restaurant) as id_restaurant,
        (select id from dim_locations where address = delivered_to) as id_delivered_to_location       
    FROM staging_eats);
""")

load_fact_rides = ("""
    INSERT INTO fact_rides
    (
    SELECT
        event_id as id_ride,
        date as id_date,
    (select id from dim_users where email = userservice) as id_user,
    nvl2(amount_charged, amount_charged, 0.0) as amount_charged,
    nvl2(total, total, 0.0) as total,
    nvl2(subtotal, subtotal, 0.0) as subtotal,
    nvl2(booking_fee, booking_fee, 0.0) as booking_fee,
    nvl2(government_contribution, government_contribution, 0.0) as government_contribution,
    nvl2(wait_time, wait_time, 0.0) as wait_time,
    nvl2(trip_fare, trip_fare, 0.0) as trip_fare,
    nvl2(discounts, discounts, 0.0) as discounts,
    nvl2(before_Taxes, before_Taxes, 0.0) as before_Taxes,
    nvl2(balance, balance, 0.0) as balance,
    nvl2(time_payment, time_payment, 0.0) as time_payment,
    nvl2(distance, distance, 0.0) as distance_payment,
    nvl2(unsettled_past_uber_trip, unsettled_past_uber_trip, 0.0) as unsettled_past_uber_trip, 
    nvl2(distance_service, distance_service, 0.0) as distance_service,   
    datediff(minute,time_from_service,time_to_service) as time_service,
    time_from_service as id_time_from_service,
    time_to_service as id_time_to_service,
    (select id from dim_locations where address = from_address) as id_from_location,
    (select id from dim_locations where address = to_address) as id_to_location
    FROM staging_rides
    );
""")

drop_staging = ("""
  DROP TABLE IF EXISTS staging_eats;
  DROP TABLE IF EXISTS staging_rides;
  DROP TABLE IF EXISTS staging_eats_items;
""")

fixing_locations = ("""
    update staging_eats
    set picked_up_from = 'Laureles 1300, Belenes Nte., 45130 Zapopan, Jal., México'
    where restaurant = 'K F C( Terraza Belenes-726)';

    update staging_eats
    set picked_up_from = 'Av. Juan Gil Preciado, Plaza La Cima, La Cima, 45134 Zapopan, Jal., México'
    where restaurant = 'Cuartode Kilo( La Cima)';

    update staging_eats
    set picked_up_from = 'Av. Juan Gil Preciado 1600 , La Cima, 45130 Zapopan, Jal., Mexico'
    where restaurant = 'Los Tarascos( La Cima)';

    update staging_eats
    set picked_up_from = 'Av Federalistas 1100-3, Colinas del Rey, 45130 Zapopan, Jal., México'
    where restaurant = 'Punto Salad';

    update staging_eats
    set picked_up_from = 'Av Torremolinos 3465 , Colinasdel Rey, 45130 Zapopan, Jal., Mexico'
    where restaurant = 'La Desayuneria';

    update staging_eats
    set picked_up_from = 'Avenida Santa Margarita #3600 Local FS-04, Colonia Residencial, 45136 Zapopan, Jal., México'
    where restaurant = 'Popeyes Real Center';

    update staging_eats
    set picked_up_from = 'Laureles 1300, Belenes Nte., 45130 Zapopan, Jal., México'
    where restaurant = 'Pizza Hut Belenes';

    update staging_eats
    set picked_up_from = 'Av. Juan Gil Preciado 1806, Los Robles, 45134 Zapopan, Jal., México'
    where restaurant = 'Starbucks( Plaza Los Robles)';
    
    update staging_eats
    set picked_up_from = 'Calz Federalistas 2380, Jardines del Valle, 45138 Zapopan, Jal., México'
    where restaurant = 'Pollo Pepe( Jardines)';

    update staging_eats
    set picked_up_from = 'Av Valdepeñas 2380, Lomas de Zapopan, 45130 Zapopan, Jal.'
    where restaurant = 'Pollo Pepe( Lomasde Zapopan)';

    update staging_eats
    set picked_up_from = 'Av. Base Aerea 465, Nuevo México, 45132 Zapopan, Jal., México'
    where restaurant = 'Carlo Cocina Artesanal';

    update staging_eats
    set picked_up_from = 'Av Valdepeñas 8819, Real de Valdepeñas, 45130 Zapopan, Jal., México'
    where restaurant = 'Lascazuelasdelaabuela';

    update staging_eats
    set picked_up_from = 'Camino Viejo a Tesistan 1579, Santa Margarita1a Secc., 45140 Zapopan, Jal., México'
    where restaurant = 'Breakfast México| Restaurante Digital';

    update staging_eats
    set picked_up_from = 'Av. Acueducto 849, Santa Margarita1a Secc., 45140 Zapopan, Jal., México'
    where restaurant = 'Pollo Bronco( S A N T A M A R G A R I T A)';

    update staging_eats
    set picked_up_from = 'Av. Bosques de San Isidro 780, Local B-6 y B-7, La Grana Fraccionamiento, 45157 Zapopan, Jal.'
    where restaurant = 'Mia Mia Pizzería( San Isidro)';        


""")





# .....................CONSTRAINTS......................
# alter table dim_products_order ADD CONSTRAINT fk_dim_products_order FOREIGN KEY(id_product) REFERENCES dim_products(id);
# alter table dim_products_order ADD CONSTRAINT fk_dim_products_order_fact_eats FOREIGN KEY(id_order) REFERENCES fact_eats(id_order);
# alter table dim_restaurants ADD CONSTRAINT fk_dim_restaurants_dim_locations FOREIGN KEY(id_location) REFERENCES dim_locations(id);
# alter table fact_eats ADD CONSTRAINT fk_fact_eats_dim_restaurants FOREIGN KEY(id_restaurant) REFERENCES dim_restaurants(id);
# alter table fact_eats ADD CONSTRAINT fk_fact_eats_dim_locations FOREIGN KEY(id_delivered_to_location) REFERENCES dim_locations(id);
# alter table fact_eats ADD CONSTRAINT fk_fact_eats_dim_users FOREIGN KEY(id_user) REFERENCES dim_users(id);
# alter table fact_eats ADD CONSTRAINT fk_fact_eats_dim_times FOREIGN KEY(id_date) REFERENCES dim_times(date);
# alter table fact_rides ADD CONSTRAINT fk_fact_rides_dim_users FOREIGN KEY(id_user) REFERENCES dim_users(id);
# alter table fact_rides ADD CONSTRAINT fk_fact_rides_dim_location1 FOREIGN KEY(id_from_location) REFERENCES dim_locations(id);
# alter table fact_rides ADD CONSTRAINT fk_fact_rides_dim_location2 FOREIGN KEY(id_to_location) REFERENCES dim_locations(id);
# alter table fact_rides ADD CONSTRAINT fk_fact_rides_dim_times FOREIGN KEY(id_date) REFERENCES dim_times(date);
# alter table fact_rides ADD CONSTRAINT fk_fact_rides_dim_times1 FOREIGN KEY(id_time_from_service) REFERENCES dim_times(date);
# alter table fact_rides ADD CONSTRAINT fk_fact_rides_dim_times2 FOREIGN KEY(id_time_to_service) REFERENCES dim_times(date);
# alter table dim_times ADD CONSTRAINT fk_dim_times_dim_year FOREIGN KEY(year) REFERENCES dim_year(id);
# alter table dim_times ADD CONSTRAINT fk_dim_times_dim_month FOREIGN KEY(month) REFERENCES dim_month(id);
# alter table dim_times ADD CONSTRAINT fk_dim_times_dim_weekday FOREIGN KEY(weekday) REFERENCES dim_weekday(id);
# alter table dim_times ADD CONSTRAINT fk_dim_times_dim_hour FOREIGN KEY(hour) REFERENCES dim_hour(id);
