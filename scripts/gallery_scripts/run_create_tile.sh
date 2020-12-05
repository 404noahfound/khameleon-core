# 250 x 250 images
python3 scripts/gallery_scripts/create_tile.py data/progressive_f100 --dim 2500 --factor 10 --repeat 25 --fname data/tile_250_250
# 1000 x 1000 images
python3 scripts/gallery_scripts/create_tile.py data/progressive_f100 --dim 10000 --factor 10 --repeat 100 --fname data/tile_1M
