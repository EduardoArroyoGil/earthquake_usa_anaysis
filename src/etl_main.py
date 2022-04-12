import sys
import time
import src.etl_scripts.create_db as create_db
# import src.etl_scripts.extract_soruce as extract_source
# import src.etl_scripts.link_table_creator as link_table_creator
import src.etl_scripts.insert_data_into_db as insert_data_into_db

sys.path.append("../")

sys.stdout = open("../src/log_etl_main_earthquake_vs_gdp.txt", "w")

print('START MAIN PROCEDURE\n')
general_start = time.process_time()

create_db
# extract_source
# link_table_creator
insert_data_into_db

duration = round((time.process_time() - general_start)/60, 2)
print(f'FINISH MAIN PROCEDURE (general duration: {duration} min)')

sys.stdout.close()