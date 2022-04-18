create_table_earthquakes = '''
CREATE TABLE IF NOT EXISTS `earthquakes`.`f_earthquakes` (
 `id_earthquake`		VARCHAR(36)
, `key_earthquake_gdp`	VARCHAR(36)
, `time`  				TIMESTAMP
, `year`				YEAR
, `latitude` 			FLOAT 
, `longitude` 			FLOAT
, `depth` 				FLOAT
, `mag` 				FLOAT
, `magType` 			TEXT
, `nst`					FLOAT
, `gap`					FLOAT
, `dmin`				FLOAT
, `rms`					FLOAT
, `net`					TEXT
, `id`					TEXT
, `place`				TEXT
, `type`				TEXT
, `horizontalError`		FLOAT
, `depthError`			FLOAT
, `magError`			FLOAT
, `magNst`				FLOAT
, `status`				TEXT
, `locationSource`		TEXT
, `magSource`			TEXT
, `updated_date`		TIMESTAMP
, `inserted_date`		TIMESTAMP
, PRIMARY KEY (`id_earthquake`)
, FOREIGN KEY (`key_earthquake_gdp`) REFERENCES `earthquakes`.`link_table`(`key_earthquake_gdp`)
, INDEX `fk_idx_key_earthquake_gdp`(`key_earthquake_gdp` ASC));

'''

create_table_gdp = '''
CREATE TABLE IF NOT EXISTS `earthquakes`.`f_gdp` (
`id_gdp`				VARCHAR(36)
, `key_gdp_earthquake`	VARCHAR(36)
, `year`				YEAR
, `gdp`					FLOAT
, `state_code`			VARCHAR(2)
, `state_name`			TEXT
, `longitude`			FLOAT
, `latitude`			FLOAT
, `inserted_date`		TIMESTAMP
, PRIMARY KEY (`id_gdp`)
, FOREIGN KEY (`key_gdp_earthquake`) REFERENCES `earthquakes`.`link_table`(`key_gdp_earthquake`)
, INDEX `fk_idx_key_gdp_earthquake`(`key_gdp_earthquake` ASC));
'''

create_link_table = '''
CREATE TABLE IF NOT EXISTS `earthquakes`.`link_table` (
`key_earthquake_gdp`				VARCHAR(36)
, `key_gdp_earthquake`	VARCHAR(36)
, INDEX `fk_idx_key_earthquake_gdp`(`key_earthquake_gdp` ASC)
, INDEX `fk_idx_key_gdp_earthquake`(`key_gdp_earthquake` ASC));
'''