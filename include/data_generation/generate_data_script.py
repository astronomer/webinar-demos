import pandas as pd
import numpy as np
from datetime import datetime, timedelta

FACILITY_LOCATIONS = [
    "North Facility", "South Facility", "East Facility", "West Facility",
    "Central Facility", "Northeast Facility", "Northwest Facility", "Southeast Facility"
]
DEPARTMENTS = ["Manufacturing", "Quality Control", "Research & Development", "Logistics"]
GREEN_ENERGY = [True, False]

def generate_mock_data(start_date, num_days):
    data = []

    for day in range(num_days):
        current_date = start_date + timedelta(days=day)
        for location in FACILITY_LOCATIONS:
            for department in DEPARTMENTS:
                timestamp = current_date.strftime('%Y-%m-%d')
                power_use = round(np.random.uniform(1000, 5000), 2)  # kWh
                green_energy_initiative = np.random.choice(GREEN_ENERGY)
                
                if green_energy_initiative:
                    power_given_back = round(np.random.uniform(100, 500), 2)  # kWh
                else:
                    power_given_back = np.nan
                
                output_of_product = round(np.random.uniform(50, 500), 2)
                
                data.append([
                    timestamp,
                    location,
                    department,
                    power_use,
                    green_energy_initiative,
                    power_given_back,
                    output_of_product
                ])

    return data


start_date = datetime.now() - timedelta(days=30)  
num_days = 30  

mock_data = generate_mock_data(start_date, num_days)

df = pd.DataFrame(mock_data, columns=[
    "TS",
    "ProductionFacilityLocation",
    "Department",
    "AmtPowerUse",
    "IsGreenEnergyInitiative",
    "AmtPowerReturnedToGrid",
    "AmtProductOutput"
])

df.to_csv('mock_data.csv', index=False)
