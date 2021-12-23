import json

with open("pair_volumes.json", "r") as file:
    volumes = json.load(file)

trading_volume_2021 = 0
for volume in volumes:
    trading_volume_2021 += volumes[volume]

print(trading_volume_2021)

