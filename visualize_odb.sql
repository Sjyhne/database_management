
CREATE TABLE target_type (
    target_type VARCHAR PRIMARY KEY
);


CREATE TABLE entity (
    entity_name VARCHAR PRIMARY KEY
);

CREATE TABLE attacker (
    group_name VARCHAR PRIMARY KEY
);

CREATE TABLE region (
    region VARCHAR PRIMARY KEY
);

CREATE TABLE country (
    country VARCHAR PRIMARY KEY,
    region VARCHAR,
    FOREIGN KEY (region) REFERENCES region(region)
);

CREATE TABLE provstate (
    provstate VARCHAR PRIMARY KEY,
    country VARCHAR,
    FOREIGN KEY (country) REFERENCES country(country)
);

CREATE TABLE city (
    city VARCHAR PRIMARY KEY,
    provstate VARCHAR,
    FOREIGN KEY (provstate) REFERENCES provstate(provstate)
);
    
CREATE TABLE attack_type (
    attack_type VARCHAR PRIMARY KEY
);

CREATE TABLE weapon_sub_type (
    weapon_sub_type VARCHAR PRIMARY KEY
);

CREATE TABLE weapon_type (
    weapon_type VARCHAR PRIMARY KEY,
    weapon_sub_type VARCHAR,
    FOREIGN KEY (weapon_sub_type) REFERENCES weapon_sub_type(weapon_sub_type)
);

CREATE TABLE target (
    target VARCHAR PRIMARY KEY,
    target_nat VARCHAR,
    target_type VARCHAR,
    entity_name VARCHAR,
    FOREIGN KEY (target_nat) REFERENCES country(country),
    FOREIGN KEY (target_type) REFERENCES target_type(target_type),
    FOREIGN KEY (entity_name) REFERENCES entity(entity_name)
);

CREATE TABLE event (
    event_id VARCHAR PRIMARY KEY,
    group_name VARCHAR,
    target VARCHAR,
    FOREIGN KEY (group_name) REFERENCES attacker(group_name),
    FOREIGN KEY (target) REFERENCES target(target)
);

CREATE TABLE date (
    year INTEGER,
    month INTEGER,
    day INTEGER,
    PRIMARY KEY(year, month, day)
);

CREATE TABLE city_population(
    year INTEGER,
    city VARCHAR PRIMARY KEY,
    population INTEGER,
    FOREIGN KEY (city) REFERENCES city(city)
);

CREATE TABLE country_population(
    year INTEGER,
    country VARCHAR PRIMARY KEY,
    population INTEGER,
    FOREIGN KEY (country) REFERENCES country(country)
);

CREATE TABLE event_info (
    event_id VARCHAR PRIMARY KEY,
    city VARCHAR,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    success INTEGER,
    suicide INTEGER,
    host_kid INTEGER,
    nhost_kid INTEGER,
    host_kid_hours INTEGER,
    host_kid_days INTEGER,
    ransom INTEGER,
    ransom_amt INTEGER,
    ransom_amt_paid INTEGER,
    longitude FLOAT,
    latitude FLOAT,
    nperps INTEGER,
    nperps_cap INTEGER,
    individual INTEGER,
    total_killed INTEGER,
    perps_killed INTEGER,
    total_wounded INTEGER,
    perps_wounded INTEGER,
    property_dmg INTEGER,
    property_dmg_value INTEGER,
    weapon_type VARCHAR,
    attack_type VARCHAR,
    country VARCHAR,
    date DATE,
    FOREIGN KEY (event_id) REFERENCES event(event_id),
    FOREIGN KEY (city) REFERENCES city(city),
    FOREIGN KEY (weapon_type) REFERENCES weapon_type(weapon_type),
    FOREIGN KEY (attack_type) REFERENCES attack_type(attack_type),
    FOREIGN KEY (year, month, day) REFERENCES date(year, month, day)
);