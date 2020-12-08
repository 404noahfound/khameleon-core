import click
import json
import pdb
import os.path as ospath
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

def draw_nblocks_graph(data, output_path):
    start_nblocks = [item['start_nblocks'] for item in data.values()]
    start_nblocks_count = defaultdict(int)
    for nblock in start_nblocks:
        start_nblocks_count[nblock] += 1
    count_sum = sum(start_nblocks_count.values())
    start_block_percentage = [item / count_sum for item in start_nblocks_count.values()]
    y_pos = list(start_nblocks_count.keys())

    plt.bar(y_pos, start_block_percentage, align='center')
    plt.xticks(y_pos)
    plt.xlabel('Number of blocks prefetched')
    plt.ylabel('Percentage of Queries')
    # plt.title('Number of blocks prefetched')
    plt.savefig(output_path)
    plt.clf()
    return

def draw_half_blocks_graph(data, output_path):
    MAX_TIME = 2000
    half_blocks_time = [item['fair_quality_time'] for item in data.values() if 'fair_quality_time' in item]
    # pdb.set_trace()
    half_blocks_time.sort()
    half_blocks_time = [min(item, MAX_TIME) for item in half_blocks_time]
    hx, hy, _ = plt.hist(half_blocks_time, bins=10, range=(0, MAX_TIME))
    plt.yticks(np.arange(1, 6))
    print(half_blocks_time)
    print(hx, hy)
    plt.xlabel('Time to fetch half of the blocks in the query (Millisecond)')
    plt.ylabel('Number of queries')
    plt.savefig(output_path)
    plt.clf()

@click.command()
@click.argument('log_path')
@click.option('--output_path', default='./log/analysis')
@click.option('--prefix', default='', help='output file name prefix')
def main(log_path, output_path, prefix):
    with open(log_path, 'r') as f:
        data = json.load(f)
    draw_nblocks_graph(data, ospath.join(output_path, prefix + "-prefetch_blocks.png"))
    draw_half_blocks_graph(data, ospath.join(output_path, prefix + "-half_blocks.png"))

if __name__ == "__main__":
    main()