""" Simple cli interface.

Example usage:
$ python -O cli.py --nodes 4 --partitions 2 --rounds 4 --workers 16 -dryrun
"""
from generator import Generator
import argparse
import logging
import multiprocessing

if __name__ == '__main__':
    multiprocessing.freeze_support()

    # Input arguments
    parser = argparse.ArgumentParser(description='Twins Generator.')
    parser.add_argument(
        '--nodes', help='the number of nodes', type=int
    )
    parser.add_argument(
        '--partitions', help='the number of partitions', type=int
    )
    parser.add_argument(
        '--rounds', help='the number of rounds', type=int
    )
    parser.add_argument(
        '--testcases_per_file',
        help='number of testcases to print per file (default 100)',
        type=int,
        default=100
    )
    parser.add_argument(
        '--path',
        help='directory path where to print the testcases (default "./")',
        default='./'
    )
    parser.add_argument(
        '--index',
        help='the index of the machine (default "1")',
        type=int,
        default=1
    )
    parser.add_argument(
        '--machines',
        help='the total number of machines (default "1")',
        type=int,
        default=1
    )
    parser.add_argument(
        '--workers',
        help='the number of processes (default "1")',
        type=int,
        default=1
    )
    parser.add_argument(
        '-v',
        dest='verb',
        action='store_true',
        help='activate verbose logging'
    )
    parser.add_argument(
        '-dryrun',
        dest='dryrun',
        action='store_true',
        help='do not print any file'
    )
    args = parser.parse_args()

    # Sanitize
    if args.nodes is None or args.partitions is None or args.rounds is None:
        parser.error(
            'arguments "nodes", "rounds", and "partitions" must be supplied'
        )
    if args.nodes <= 0 or args.partitions <= 0 or args.rounds <= 0:
        parser.error(
            'arguments "nodes", "rounds", and "partitions" must be positif'
        )
    if args.testcases_per_file <= 0:
        parser.error(
            'argument "testcases_per_file" must be strictly positif'
        )
    if args.index <= 0 or args.index > args.machines:
        parser.error(
            'invalid machine index or number of machines'
        )
    if args.workers <= 0:
        parser.error(
            'argument "workers" must be strictly positif'
        )

    # Apply settings and run the generator
    logging.basicConfig(
        level=logging.DEBUG if args.verb else logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s"
    )

    generator = Generator(
        args.nodes,
        args.partitions,
        args.rounds,
        filter=None,
        folder_path=args.path,
        machine_index=args.index,
        number_of_machines=args.machines
    )
    generator.run(
        workers=args.workers,
        dryrun=args.dryrun,
        testcases_per_file=args.testcases_per_file
    )
