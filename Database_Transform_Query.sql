# call the split_coulmn function  
CALL split_column();

-- combining the scirt_job and temp_table into one table,
-- so that in every row the job id is correspond to one route. 

-- create a table have the same structure as the scirt_job table,
-- since we want to contain the information in the same way as the scirt_job table does. 
DROP TABLE IF EXISTS scirt_job_table;
CREATE 	TABLE scirt_job_table LIKE scirt_job;
#for the next inserting value into the table with duplicated job_id, need to to set the job_id column as not primary key 
ALTER TABLE scirt_job_table DROP PRIMARY KEY;

-- change the column name routes to route,
-- where later on we will insert a single route value into the column.
ALTER TABLE scirt_job_table 
RENAME COLUMN routes TO route;


-- insert value in the the scirt_job_table we just created
-- by matching the job_id in both the scirt_job table and temp_table. 
-- so that in each row we have one job_id and one route. eliminate multivalued attribute. 
INSERT INTO scirt_job_table (job_id, description, route, locality, delivery_team, start_date, end_date)
SELECT job_id, description, route, locality, delivery_team, start_date, end_date 
FROM 
(SELECT sj.job_id, temp.route, sj.description, sj.locality, sj.delivery_team, sj.start_date, sj.end_date
FROM scirt_job AS sj INNER JOIN temp_table AS temp
ON sj.job_id = temp.job_id
ORDER BY sj.job_id) AS t;

-- add a column as primary key for the table to keep track of evey row
-- this column also serves as a marker for each row, for later on we delect duplicate information. 
ALTER TABLE scirt_job_table ADD COLUMN row_id int NOT NULL AUTO_INCREMENT PRIMARY KEY;


-- Now we can just need to work with the scirt_job_table. 
-- change to safe update setting to delete multiple rows. 
SET SQL_SAFE_UPDATES = 0;

-- remove duplication rows
-- use the row_number function, partition the table by
-- the route and job_id, and count the occurence of both as 
-- ROW_NUM, so that we can just delect the row where the ROW_NUM is great than 1. 

DELETE FROM scirt_job_table 
WHERE row_id IN
( 
SELECT row_id FROM 
(SELECT row_id,job_id, route,
ROW_NUMBER() OVER(PARTITION BY route, job_id ORDER BY row_id) as ROW_NUM
FROM scirt_job_table) 
AS z
WHERE ROW_NUM > 1);


-- we can not delete the scirt_job and temp_table from the schema
DROP TABLE scirt_job, temp_table;


-- adding a new primary key to the scirt_job_table, droping the row_id column;
ALTER TABLE scirt_job_table
DROP COLUMN row_id,
ADD COLUMN road_job_id INT NOT NULL AUTO_INCREMENt,
ADD CONSTRAINT PRIMARY KEY(road_job_id);


-- BEFORE transforming the table into a table, we need to figure out the 
-- relationship between attribute and their functional dependency
-- my way of checking the dependency is by checking the unique value of each 
-- attribute in the tabel

-- checking the functional depency on scirt_job_table:
SELECT * FROM scirt_job_table;
SELECT COUNT(DISTINCT route) FROM scirt_job_table;
SELECT COUNT(DISTINCT job_id ) FROM scirt_job_table;
SELECT COUNT(DISTINCT delivery_team) FROM scirt_job_table;
SELECT COUNT(DISTINCT start_date) FROM scirt_job_table;
SELECT COUNT(DISTINCT end_date) FROM scirt_job_table;
SELECT COUNT(DISTINCT locality) FROM scirt_job_table;
SELECT COUNT(DISTINCT description) FROM scirt_job_table;

-- conclusion:
-- given job_id, we can determint the delivery_team, start_date, end_date, locality and description
-- delivery_team, start_date, end_date, locality and description are functionally dependt on job_id
-- However, a route be done in many jobs, and a job can be done on many routes
-- Therefore, the route and job_id have an many to many relationship, we will need an associative table. 
-- -----------------------------------------------

-- Transform scirt_job_table into a set of tables.
-- the scirt_job_table has been transformed into 3 tables:
-- route_job_table: which have the information about the job have been done on each route
-- job_assignment_date : contains information about each repair job : job_id, start and end date, delivery team and description
-- delivery_team : all the team has been given a team_id
-- ------------------------------------------------

-- CREAT Table `scirt_jobs_bound`.`delivery_team` 
-- and adding all delivery team value into the table.

CREATE TABLE IF NOT EXISTS `scirt_jobs_bound`.`delivery_team` (
  `delivery_team_id` INT NOT NULL AUTO_INCREMENT,
  `delivery_team_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`delivery_team_id`));
  
  INSERT INTO delivery_team (delivery_team_name)
  SELECT DISTINCT delivery_team FROM scirt_job_table;


-- Table `scirt_jobs_bound`.`job_assignment_date`
CREATE TABLE IF NOT EXISTS `scirt_jobs_bound`.`job_assignment_date` (
  `job_id` INT NOT NULL,
  `start_date` DATE NOT NULL,
  `end_date` DATE NOT NULL,
  `delivery_team_id` INT NOT NULL,
  `description` VARCHAR(400) NOT NULL,
  PRIMARY KEY (`job_id`));
   
-- inserting value into the table. 
  SELECT * FROM job_assignment_date;
  INSERT INTO job_assignment_date ( job_id, start_date, end_date, delivery_team_id,description) 
  SELECT DISTINCT job_id, start_date, end_date, delivery_team_id,description FROM scirt_job_table
  JOIN delivery_team 
  ON delivery_team.delivery_team_name = scirt_job_table.delivery_team;
    
-- Beacuse the route_job_table will be using the road_name_table from the
-- chch_street_address table sets, the table creation query is at the end of 
-- the chch_street_address table sets.  

-- ------------------------------------------------------------------------------------------------------------------------
-- BEFORE transforming the chch_street_address table, we neew to check the fuctional dependancy for the attributes

SELECT * FROM chch_street_address;
SELECT COUNT(DISTINCT road_section_id) FROM chch_street_address;
SELECT COUNT(DISTINCT suburb_locality) FROM chch_street_address;
SELECT COUNT(DISTINCT road_name) FROM chch_street_address;
SELECT COUNT(DISTINCT address_id) FROM chch_street_address;

-- conclusion:
-- locality is functionally dependent on road_name and road_section_id
-- -----------------------------------------------
-- Transform chch_street_address into a set of tables.
-- the chch_street_address has been transformed into 3 tables:
-- street_address table: contains the details information of a single hoem address
-- road_name table: contains all the road_name in Chrischurch
-- locality table : has all the surburb locality of Chrischurch
-- ------------------------------------------------


-- Table `scirt_jobs_bound`.`locality`
-- give each surburb an id
CREATE TABLE IF NOT EXISTS locality (
  `locality_id` INT NOT NULL AUTO_INCREMENT,
  `suburb_locality` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`locality_id`));

-- insert data into data, remove empty string value, since it's not relevant for us
INSERT INTO locality (suburb_locality)
SELECT DISTINCT suburb_locality FROM chch_street_address;
SET SQL_SAFE_UPDATES = 0;
DELETE FROM locality WHERE suburb_locality = '';
    


-- Table `scirt_jobs_bound`.`road_name_table`
-- give each road an id 
CREATE TABLE IF NOT EXISTS road_name_table (
  `road_name_id` INT NOT NULL AUTO_INCREMENT,
  `road_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`road_name_id`));
  
  -- inserting value into table
  INSERT INTO road_name_table (road_name)
  SELECT DISTINCT road_name FROM chch_street_address;
    
    
-- Now we can create the route_job table, which connects the road name and 
-- job has been done on it. 
-- Table `scirt_jobs_bound`.`route_job_table`
CREATE TABLE IF NOT EXISTS `scirt_jobs_bound`.`route_job_table` (
  `route_job_id` INT NOT NULL AUTO_INCREMENT,
  `route_id` INT NOT NULL,
  `job_id` INT NOT NULL,
  PRIMARY KEY (`route_job_id`));
  
  -- insert value 
  INSERT INTO route_job_table (route_id,job_id)
  SELECT
  road_name_table.road_name_id,
  scirt_job_table.job_id
  FROM scirt_job_table, road_name_table
  WHERE road_name_table.road_name = scirt_job_table.route;

  SELECT * FROM route_job_table;

    
-- Table `scirt_jobs_bound`.`street_address`
--  detail address table 
CREATE TABLE IF NOT EXISTS `scirt_jobs_bound`.`street_address` (
  `address_id` INT UNSIGNED NOT NULL,
  `unit_value` VARCHAR(20) CHARACTER SET 'utf8mb4' NULL DEFAULT NULL,
  `address_number` INT NULL DEFAULT NULL,
  `address_number_suffix` VARCHAR(5) CHARACTER SET 'utf8mb4' NULL DEFAULT NULL,
  `address_number_high` VARCHAR(5) CHARACTER SET 'utf8mb4' NULL DEFAULT NULL,
  `road_name_id` INT NOT NULL,
  `road_section_id` INT NULL DEFAULT NULL,
  `locality_id` INT NOT NULL,
  PRIMARY KEY (`address_id`));
  
-- insert value into table, 
-- using the road_name_id instead of road_name,
-- usnig locality_id instead of suburb_locality

 INSERT INTO street_address 
 SELECT address_id, unit_value, address_number, 
 address_number_suffix, 
 address_number_high, 
 road_name_table.road_name_id,
 road_section_id,
 locality_id
 FROM chch_street_address, road_name_table, locality
  WHERE chch_street_address.suburb_locality = locality.suburb_locality
  and chch_street_address.road_name = road_name_table.road_name; 


-- ------------------------------------------------------------------------------------------------------------
-- Now we have all the tables with data that needed, the below seciton is to set the constraints
-- of these table, connecting them together. 
-- ------------------------------------------------------------------------------------------------------------

-- -----------------------------
-- street_address table
-- -----------------------------
-- setting the road_id and locality_id as foreign keys,
-- referencing ot he locality and road_name table.
ALTER TABLE street_address 
ADD CONSTRAINT road_name_id_fk
     FOREIGN KEY (road_name_id)
     REFERENCES road_name_table(road_name_id)
     ON DELETE NO ACTION
     ON UPDATE NO ACTION,
ADD CONSTRAINT locality_id_fk
    FOREIGN KEY (locality_id)
    REFERENCES locality (locality_id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION;
    
-- ---------------------------
-- route_job_table
-- --------------------------=
-- setting the route_id and job_id as foreign keys
-- refering to the road_name_table and job_assignment table 
ALTER TABLE route_job_table 
ADD CONSTRAINT route_id_fk
     FOREIGN KEY (route_id)
     REFERENCES road_name_table(road_name_id)
     ON DELETE NO ACTION
     ON UPDATE NO ACTION,
ADD CONSTRAINT job_id_fk
    FOREIGN KEY (job_id)
    REFERENCES job_assignment_date (job_id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION;
    
    
-- ---------------------------
-- job_assignment_date
-- --------------------------=
-- setting the delivery_team_id as foreign key
-- refering to the delivery_team table
ALTER TABLE job_assignment_date
ADD CONSTRAINT delivery_team_id_fk
     FOREIGN KEY (delivery_team_id)
     REFERENCES delivery_team(delivery_team_id)
     ON DELETE NO ACTION
     ON UPDATE NO ACTION;
     
-- all the foreign keys have been set to on update and on delete no action, because for this database design, 
-- there is no need to change the primary in reghlarity, it helps to retain consistency of the database. 
-- ----------------------------------------------------------------------------------------------------------
-- Now we have all the table we need, the original chch_street_address and scirt_job_table can be removed.
-- -----------------------------------------------------------------------------------------------------------
DROP TABLE chch_street_address,scirt_job_table
     
    
    
