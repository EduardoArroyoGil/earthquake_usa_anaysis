import sys
import os
sys.path.append("../")

# sys.stdout = open("log_main_earthquake_vs_gdp.txt", "w")

sys.stdout = open("log_main_earthquake_vs_gdp.txt", "w")
print('START MAIN PROCEDURE')

from src.generates_csv import generate_csv
# from src.gdp_states_usa import generate_gdp_state
# from src.generate_final_data import generate_final_data

print('FINISH MAIN PROCEDURE')




sys.stdout.close()