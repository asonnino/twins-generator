""" Generator benchmark.

Usage:
$ python -O benchmark.py  # Run a fast benchmark (should take a few minutes)
$ python -O benchmark.py full  # Run a detailed benchmark and plot results
"""
from generator import Generator
from datetime import timedelta
import matplotlib.pyplot as plt
from json import dumps
import time
import sys


def run_benchmark(gen, workers, testcases_per_file):
    print(f'Generating {gen.testcases_length} testcases..')
    start_time = time.perf_counter()
    gen.run(dryrun=True, workers=workers, testcases_per_file=testcases_per_file)
    elapsed_time = time.perf_counter() - start_time
    print(f'Elapsed time: {str(timedelta(seconds=elapsed_time))}')
    return elapsed_time


def plot(gen, data, workers, testcases_per_file):
    labels = {k: f'{k} testcases per file' for k in testcases_per_file}
    [plt.plot(workers, v, label=labels[k]) for k, v in measure.items()]
    plt.xlabel('Number of processes')
    plt.ylabel('Time (min)')
    plt.title(f'Generator time for {gen.testcases_length} testcases')
    plt.legend(loc='upper right')
    plt.ylim(0, max(max(list(measure.values())))*1.2)
    plt.savefig(f'generator-{gen.testcases_length}.pdf')


if __name__ == '__main__':
    gen = Generator(4, 2, 6)  # About 11M testcases

    if len(sys.argv) > 1 and sys.argv[1] == 'full':
        print('Running full benchmark, this may take a long time...')
        workers = [i for i in range(1, 11)]
        testcases_per_file = [1000, 10000, 100000]
        measure = {}
        for t in testcases_per_file:
            measure[t] = [run_benchmark(gen, x, t) / 60 for x in workers]
        with open(f'generator-{gen.testcases_length}.txt', 'w') as file:
            file.write(dumps(measure))
        plot(gen, measure, workers, testcases_per_file)
        print('Done.')
    else:
        run_benchmark(gen, 8, 2000)
