"""
## Create Bunny Data
"""

import pandas as pd
import numpy as np

np.random.seed(42)

n_bunnies = 300
bunny_names = [
    "Fluffy",
    "Cotton",
    "Snowball",
    "Hopper",
    "Nibbles",
    "Peanut",
    "Buttercup",
    "Whiskers",
    "Coco",
    "Binky",
]


ids = range(1, n_bunnies + 1)
names = np.random.choice(bunny_names, n_bunnies)
breeds = np.random.choice(
    ["Lop", "Dutch", "Netherland Dwarf", "Rex", "Flemish Giant"], n_bunnies
)
cuteness = np.random.choice([9, 10], n_bunnies)
fur_colors = np.random.choice(["white", "brown", "black", "grey", "spotted"], n_bunnies)
favorite_foods = np.random.choice(["carrots", "lettuce", "kale", "apples"], n_bunnies)

ear_lengths = np.random.uniform(5, 15, n_bunnies)
slope = 10
intercept = 50
noise = np.random.normal(0, 5, n_bunnies)
hops_per_hour = slope * ear_lengths + intercept + noise


csv1_data = pd.DataFrame({"ID": ids, "Bunny Name": names, "Breed": breeds})
csv2_data = pd.DataFrame(
    {
        "ID": ids,
        "Cuteness": cuteness,
        "Number of Hops per Hour": hops_per_hour,
        "FavFood": favorite_foods,
    }
)
csv3_data = pd.DataFrame(
    {"ID": ids, "Ear Length": ear_lengths, "Fur Color": fur_colors}
)


csv1_path = "bunnies_base.csv"
csv2_path = "bunnies_observed.csv"
csv3_path = "bunnies_measurement.csv"

csv1_data.to_csv(csv1_path, index=False)
csv2_data.to_csv(csv2_path, index=False)
csv3_data.to_csv(csv3_path, index=False)

csv1_path, csv2_path, csv3_path
