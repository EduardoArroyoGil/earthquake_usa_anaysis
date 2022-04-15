import sys
import time
import src.analysis_scripts.generates_csv as gcsv
import src.analysis_scripts.gdp_states_usa as gstatesusa
import src.analysis_scripts.generate_final_data as gfinaldata
import src.analysis_scripts.analysis as analysis

sys.path.append("../")
sys.stdout = open("../src/log_main_earthquake_vs_gdp.txt", "w")

print('START MAIN PROCEDURE\n')
general_start = time.process_time()

gcsv.generate_csv()
gstatesusa.generate_gdp_state()
gfinaldata.generate_final_data()
analysis.analysis()


duration = round((time.process_time() - general_start)/60, 2)
print(f'FINISH MAIN PROCEDURE (general duration: {duration} min)')

sys.stdout.close()