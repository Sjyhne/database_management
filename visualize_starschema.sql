CREATE TABLE time_dim (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    PRIMARY KEY(year, month, day)
);

CREATE TABLE location_dim (   
    latitude FLOAT,
    longitude FLOAT,
    region VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    provstate VARCHAR NOT NULL,
    city VARCHAR NOT NULL,
    PRIMARY KEY(region, country, provstate, city)
);

CREATE TABLE event_dim (
    event_id VARCHAR PRIMARY KEY,
    attack_type VARCHAR,
    success INTEGER NOT NULL,
    suicide INTEGER NOT NULL,
    weapon_type VARCHAR NOT NULL,
    individual INTEGER NOT NULL,
    nperps INTEGER, 
    nperps_cap INTEGER,
    host_kid INTEGER NOT NULL,
    nhost_kid INTEGER,
    host_kid_hours INTEGER,
    host_kid_days INTEGER,
    ransom INTEGER NOT NULL,
    ransom_amt INTEGER,
    ransom_amt_paid INTEGER,
    total_killed INTEGER,
    perps_killed INTEGER,
    total_wounded INTEGER,
    perps_wounded INTEGER,
    property_dmg_value INTEGER,
    city_population INTEGER,
    country_population INTEGER
);

CREATE TABLE target_dim (
    target VARCHAR PRIMARY KEY,
    target_nat VARCHAR,
    target_entity VARCHAR,
    target_type VARCHAR
);

CREATE TABLE group_dim (
    group_name VARCHAR NOT NULL,      
    PRIMARY KEY(group_name)
);

CREATE TABLE fact (
    event_id VARCHAR NOT NULL,
    group_name VARCHAR NOT NULL,
    target VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    region VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    provstate VARCHAR NOT NULL,
    city VARCHAR NOT NULL,
    total_killed INTEGER NOT NULL,
    perps_killed INTEGER NOT NULL,
    property_damage INTEGER NOT NULL,
    PRIMARY KEY (event_id, group_name, target, year, month, day, region, country, provstate, city),
    FOREIGN KEY (event_id) REFERENCES event_dim(event_id),
    FOREIGN KEY (group_name) REFERENCES group_dim(group_name),                        
    FOREIGN KEY (year, month, day) REFERENCES time_dim(year, month, day),
    FOREIGN KEY (region, country, provstate, city) REFERENCES location_dim(region, country, provstate, city),
    FOREIGN KEY (target) target_dim(target)
);