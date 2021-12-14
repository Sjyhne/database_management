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
    region_name VARCHAR PRIMARY KEY
);

CREATE TABLE country (
    country_name VARCHAR PRIMARY KEY,
    region_name VARCHAR,
    FOREIGN KEY (region_name) REFERENCES region(region_name)
);

CREATE TABLE provstate (
    provstate_name VARCHAR PRIMARY KEY,
    country_name VARCHAR,
    FOREIGN KEY (country_name) REFERENCES country(country_name)
);

CREATE TABLE city (
    city_name VARCHAR PRIMARY KEY,
    provstate_name VARCHAR,
    FOREIGN KEY (provstate_name) REFERENCES provstate(provstate_name)
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
    target_entity VARCHAR,
    FOREIGN KEY (target_nat) REFERENCES country(country_name),
    FOREIGN KEY (target_type) REFERENCES target_type(target_type),
    FOREIGN KEY (target_entity) REFERENCES entity(entity_name)
);

CREATE TABLE event (
    event_id INTEGER PRIMARY KEY,
    group_name VARCHAR,
    target VARCHAR,
    FOREIGN KEY (group_name) REFERENCES attacker(group_name),
    FOREIGN KEY (target) REFERENCES target(target)
);


CREATE TABLE date (
    iyear INTEGER,
    imonth INTEGER,
    iday INTEGER,
    PRIMARY KEY(iyear, imonth, iday)
);

CREATE TABLE event_info (
    event_id INTEGER PRIMARY KEY,
    city_name VARCHAR,
    iyear INTEGER,
    imonth INTEGER,
    iday INTEGER,
    success INTEGER,
    suicide INTEGER,
    host_kid INTEGER,
    nhost_kid INTEGER,
    nhours INTEGER,
    ndays INTEGER,
    ransom INTEGER,
    ransom_amt INTEGER,
    ransom_amt_paid INTEGER,
    host_kid_outcome VARCHAR,
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
    prop_dmg VARCHAR,
    prop_dmg_value INTEGER,
    weapon_type VARCHAR,
    attack_type VARCHAR,
    current_country VARCHAR,
    date DATE,
    FOREIGN KEY (event_id) REFERENCES event(event_id),
    FOREIGN KEY (city_name) REFERENCES city(city_name),
    FOREIGN KEY (weapon_type) REFERENCES weapon_type(weapon_type),
    FOREIGN KEY (attack_type) REFERENCES attack_type(attack_type),
    FOREIGN KEY (iyear, imonth, iday) REFERENCES date(iyear, imonth, iday)
);

