import logging
from itertools import product
from more_itertools import ichunked, distribute
from os.path import join
from copy import deepcopy
from math import factorial as f
from multiprocessing import Process
from tempfile import TemporaryDirectory
from contextlib import nullcontext
from json import dumps


class JSONFormat:
    @classmethod
    def make(cls, generator, testcases, filter):
        """ Defines the format used to print testcases to files.

        This format needs to be understood by the Twins Executor.


        Args:
            generator (Generator): The generator instance.
            testcases (iterable): The testcases to print.
            filter (Object): A filter used to filter testcases before
                printing them to file.

        Returns:
            str: A formatted json string ready to be printed to file.
        """
        scenarios = []
        for testcase in testcases:
            if filter(testcase):
                x, y = cls._format_scenario(generator, testcase)
                scenarios += [{'round_leaders': x, 'round_partitions': y}]

        return dumps({
            'num_of_nodes': generator.number_of_nodes,
            'num_of_twins': generator.f,
            'scenarios': scenarios
        })

    @classmethod
    def _format_scenario(cls, generator, testcase):
        round_leaders, round_partitions = {}, {}
        for round_number, scenario in enumerate(testcase):
            leader, partition = scenario
            leaders = [leader]
            if leader in generator.target_nodes:
                leaders.append(generator.get_twin(leader))
            round_leaders[round_number+1] = leaders
            round_partitions[round_number+1] = partition
        return round_leaders, round_partitions


class Generator:
    def __init__(self, number_of_nodes, number_of_partitions, number_of_rounds,
                 filter=None, folder_path='./', machine_index=1,
                 number_of_machines=1):
        """ Instantiate the generator.

        Args:
            number_of_nodes (int): The number of nodes.
            number_of_partitions (int): The number of partitions.
            number_of_rounds (int): The number of rounds.
            filter (Object, optional): A filter used to filter testcases before
                printing them to file. Defaults to None.
            folder_path (str, optional): The directory path where to print the
                testcases. Defaults to './'.
            machine_index (int, optional): The index of the machine on which
                this generator instance is running. Defaults to 1.
            number_of_machines (int, optional): The total number of machines
                used to generate scenarios. Defaults to 1.

        Raises:
            TypeError: Raised upon invalid input types.
            ValueError: Raised upon invalid input values.
        """
        self.logger = logging.getLogger(name='generator')

        ok = isinstance(number_of_nodes, int)
        ok &= isinstance(number_of_partitions, int)
        ok &= isinstance(number_of_rounds, int)
        ok &= isinstance(folder_path, str)
        ok &= isinstance(machine_index, int)
        ok &= isinstance(number_of_machines, int)
        if not ok:
            message = 'Bad input types.'
            self.logger.error(f'TypeError: {message}')
            raise TypeError(message)

        ok &= number_of_nodes > 0
        ok &= number_of_partitions > 0
        ok &= number_of_rounds > 0
        ok &= machine_index > 0
        ok &= number_of_machines >= machine_index
        if not ok:
            message = 'Bad input values.'
            self.logger.error(f'ValueError: {message}')
            raise ValueError(message)

        self.number_of_nodes = number_of_nodes
        self.number_of_partitions = number_of_partitions
        self.number_of_rounds = number_of_rounds
        self.folder_path = folder_path
        self.machine_index = machine_index
        self.number_of_machines = number_of_machines
        self.filter = bool if filter is None else filter

        self.f = (self.number_of_nodes - 1) // 3
        self.nodes = [x for x in range(self.number_of_nodes+self.f)]

        if len(self.nodes) < self.number_of_partitions:
            message = (
                'There should be at least as many nodes as partitions. '
                f'Input: {len(self.nodes)} nodes and '
                f'{self.number_of_partitions} partitions.'
            )
            self.logger.error(f'ValueError: {message}')
            raise ValueError(message)

        if self.testcases_length > 1000000000:
            self.logger.warning(
                'Running the generator with these configurations will '
                'generate more than 1 billion testcases.'
            )

        self.logger.info(self.__repr__())

    @property
    def target_nodes(self):
        """ The list of nodes that have a twin.

        Returns:
            list(int): A list of nodes' indeces.
        """
        return self.nodes[:self.f]

    @property
    def testcases_length(self):
        """ Forecast the total number of testcases.

        Returns:
            int: The total number of testcases.
        """
        total = self.S(len(self.nodes), self.number_of_partitions)
        total *= len(self.target_nodes)
        total **= self.number_of_rounds
        return total

    def __repr__(self):
        twins_configs = (
            f'(number of nodes: {self.number_of_nodes}, '
            f'number of partitions: {self.number_of_partitions}, '
            f'number of rounds: {self.number_of_rounds})'
        )
        return f'''Generator instantiated with the following settings:
            \t Twins configs: {twins_configs}
            \t output directory: {self.folder_path}
            \t machine #: {self.machine_index}/{self.number_of_machines}'''

    def get_twin(self, node):
        """ Get the twin of a specific node.

        Args:
            node (int): The index of the node for which to return the twin.

        Returns:
            int: A node's index.
        """
        assert node in self.target_nodes
        return self.nodes[self.number_of_nodes+node]

    def S(self, n, k):
        """ Compute the Stirling numbers of the second kind.

        Args:
            n (int): Number of objects.
            k (int): Number of non-empty subsets.

        Returns:
            int: The Stirling numbers of the second kind.
        """
        assert isinstance(n, int) and isinstance(k, int)
        assert n > 0 and k > 0 and n >= k
        S = [(-1)**i * (f(k)//f(i)//f(k - i)) * (k-i)**n for i in range(k+1)]
        return sum(S) // f(k)

    def make_partitions(self):
        """ Find all possible ways in which n nodes can be partitioned into k
        partitions.

        This problems is known as "Stirling Number of the Second Kind".
        "In combinatorics, the Stirling numbers of the second kind tell
        us how many ways there are of dividing up a set of n objects
        (all different, or at least all labeled) into k nonempty subsets."

        E.g. for n={0,1,2} and k=2, possible partition are:
        [
            [ [0,1], [2] ],
            [ [0,2], [1] ],
            [ [1,2], [0] ],
            [ [2], [0,1] ],
            ...
        ]

        Returns:
            list: All possible partitions.
        """
        def stirling2(n, k):
            """ Provides solutions of the Stirling Number of the Second Kind.

            Args:
                n (int): The number of objects.
                k (int): The number of sets.

            Returns:
                list: All solutions of the Stirling number of the second kind.
            """
            assert n > 0 and k > 0
            if k == 1:
                return [[[x for x in range(n)]]]
            elif k == n:
                return [[[x] for x in range(n)]]
            else:
                s_n1_k1 = stirling2(n-1, k-1)
                for i in range(len(s_n1_k1)):
                    s_n1_k1[i].append([n-1])

                tmp = stirling2(n-1, k)
                k_s_n1_k = []
                for _ in range(k):
                    k_s_n1_k += deepcopy(tmp)
                for i in range(len(tmp)*k):
                    k_s_n1_k[i][i // len(tmp)] += [n-1]

                return s_n1_k1 + k_s_n1_k

        return stirling2(len(self.nodes), self.number_of_partitions)

    def combine_partitions_with_leaders(self, partitions):
        """ Find all possible ways in which we can assign leaders to partitions.

        E.g. for l={0,1} leaders, n={0,1,2} nodes and k=2 partitions, possible
        combinations are:
        [
            (0, [ [0,1], [2] ]),
            (0, [ [0,2], [1] ]),
            (1, [ [1,2], [0] ]),
            (0, [ [2], [0,1] ]),
            ...
        ]

        Args:
            partitions (list): List of all partitions.

        Yields:
            iterable: An iterator of tuples of (leader, partition)
        """
        for partition in partitions:
            for leader in self.target_nodes:
                yield (leader, partition)

    def combine_scenarios_with_rounds(self, scenarios):
        """ Combine the input parition-leader scenarios with rounds.

        E.g. for l={0,1} leaders, n={0,1,2} nodes, k=2 partitions, and r=2
        rounds, possible combinations are:
        [
            [(0, [ [0,1], [2] ]), (0, [ [0,1], [2] ])],
            [(0, [ [0,1], [2] ]), (0, [ [0,2], [1] ])],
            [(1, [ [1,2], [0] ]), (1, [ [1,2], [0] ])],
            [(0, [ [2], [0,1] ]), (1, [ [1,2], [0] ])],
            ...
        ]

        Args:
            iterable: An iterator of tuples of (leader, partition).

        Returns:
            iterable: An iterator of testcases.
        """
        return product(scenarios, repeat=self.number_of_rounds)

    def _print(self, testcases, process_id, dryrun, testcases_per_file):
        """ Used by a single process print testcases to files.

        Args:
            testcases (iterable): An iterator of testcases.
            process_id (int): The processe id.
            dryrun (bool): Whether dryrun mode is enabled.
            testcases_per_file (int, optional): The maximum number of testcases
                that can be printing int a single file.
        """
        chunks = ichunked(testcases, testcases_per_file)
        for i, chunk in enumerate(chunks):
            basename = f'testcase-{self.machine_index}-{process_id}'
            filename = f'tmp-{basename}' if dryrun else f'{basename}-{i}'
            #data = [RawFormat.make(self, x) for x in chunk if self.filter(x)]
            data = JSONFormat.make(self, chunk, self.filter)
            with open(join(self.folder_path, filename), 'a') as f:
                f.write(data)

    def print(self, testcases, dryrun, workers, testcases_per_file):
        """ Multiprocess print testcases to files.

        Args:
            testcases (iterable): An iterator of testcases.
            dryrun (bool): Whether dryrun mode is enabled.
            workers (int): The number of processes to create.
            testcases_per_file (int, optional): The maximum number of testcases
                that can be printing int a single file.
        """
        chunks = distribute(workers, testcases)
        jobs = []
        for i in range(workers):
            p = Process(
                target=self._print,
                args=(chunks[i], i, dryrun, testcases_per_file)
            )
            jobs.append(p)
            p.start()
        [p.join() for p in jobs]

    def run(self, dryrun=False, workers=1, testcases_per_file=1000):
        """ Run the generator: generate all testcases and print them to files.

        Args:
            dryrun (bool, optional): No files are actually written when dryrun
                is enabled. Defaults to False.
            workers (int, optional): The number of processes to use.
                Defaults to 1.
            testcases_per_file (int, optional): The maximum number of testcases
                that can be printing int a single file. Defaults to 1000.
        """
        ok = isinstance(dryrun, bool)
        ok &= isinstance(workers, int)
        ok &= isinstance(testcases_per_file, int)
        if not ok:
            message = 'Bad input types.'
            self.logger.error(f'TypeError: {message}')
            raise TypeError(message)

        ok &= workers > 0
        ok &= testcases_per_file > 0
        if not ok:
            message = 'Bad input values.'
            self.logger.error(f'ValueError: {message}')
            raise ValueError(message)

        self.logger.info(
            f'Generating {self.testcases_length} testcases...'
        )

        # Make partitions
        self.logger.debug(
            f'STEP 1. Finding all possible ways in which {len(self.nodes)} '
            f'nodes can be partitioned into {self.number_of_partitions} '
            'partitions...'
        )
        partitions = self.make_partitions()
        self.logger.debug(
            f'{self.number_of_nodes} nodes can be partitioned into '
            f'{self.number_of_partitions} partitions in '
            f'{self.S(len(self.nodes), self.number_of_partitions)} ways.'
        )

        # Combine partitions with leaders
        self.logger.debug(
            'STEP 2. Finding all possible ways in which '
            f'{self.S(len(self.nodes), self.number_of_partitions)} partitions '
            f'can be combined with {len(self.target_nodes)} leaders...'
        )
        scenarios = self.combine_partitions_with_leaders(partitions)
        self.logger.debug(
            f'{self.S(len(self.nodes), self.number_of_partitions)} partitions '
            f'can be combined with {len(self.target_nodes)} leaders in '
            f'{int(self.testcases_length**(1/self.number_of_rounds))} '
            f'possible ways.'
        )

        # Combine the parition-leader scenarios from above with rounds
        self.logger.debug(
            'STEP 3. Finding all possible ways in which '
            f'{int(self.testcases_length**(1/self.number_of_rounds))} '
            'parition-leader scenarios combinations can be combined with '
            f'{self.number_of_rounds} rounds...'
        )
        testcases = self.combine_scenarios_with_rounds(scenarios)
        self.logger.debug(
            f'{int(self.testcases_length**(1/self.number_of_rounds))} '
            'parition-leader scenarios can be combined with '
            f'{self.number_of_rounds} rounds in {self.testcases_length} '
            'possible ways.'
        )

        # Print the resulting testcases to files
        self.logger.debug(
            f'Printing all testcases to file using {workers} processes...'
        )
        testcases = distribute(self.number_of_machines, testcases)
        context_manager = TemporaryDirectory() if dryrun else nullcontext()
        with context_manager as directory:
            self.folder_path = self.folder_path if not dryrun else directory
            self.print(
                testcases[self.machine_index-1],
                dryrun,
                workers,
                testcases_per_file
            )

        self.logger.info(f'Finished.')
