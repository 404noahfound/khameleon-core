# python create_gallery.py progressive_z7 ~/research/scripts/artimages/progressive_data/ --z 7

from random import shuffle
from shutil import copyfile
import os
import click

@click.command()
@click.argument("base_dir") # "progressive_map"
@click.argument("folder") # "artimages/progressive_data/"
@click.option("--factor", default=32, help="factor level")
def main(base_dir, folder, factor):
    n = factor * factor
    filepaths = get_files_by_file_size(folder, reverse=False)
    filepaths = filepaths[0:n]
    shuffle(filepaths)
    create_map_gallary(base_dir, filepaths, factor)

def createFolder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def create_map_gallary(base_dir, filepaths, factor):
    idx = 0
    level_1_dir = base_dir + "/%s" % factor
    createFolder(level_1_dir)
    for x in range(factor):
        level_2_dir = level_1_dir + "/" + str(x)
        createFolder(level_2_dir)
        for y in range(factor):
            file_name = str(y) + ".jpg"
            level_3_dir = level_2_dir + "/" + file_name
            print(level_3_dir)
            img_path = filepaths[idx]
            copyfile(img_path, level_3_dir)

            idx = (idx + 1) % len(filepaths)

def get_files_by_file_size(dirname, reverse=False):
    # return list of file paths in directory sorted by file size

    # get list of files
    filepaths = []
    for basename in os.listdir(dirname):
        filename = os.path.join(dirname, basename)
        if os.path.isfile(filename):
            filepaths.append(filename)

    for i in xrange(len(filepaths)):
        filepaths[i] = (filepaths[i], os.path.getsize(filepaths[i]))

    filepaths.sort(key=lambda filename: filename[1], reverse=reverse)

    # re-populate list with just filenames
    for i in xrange(len(filepaths)):
        filepaths[i] = filepaths[i][0]

    return filepaths


main()
