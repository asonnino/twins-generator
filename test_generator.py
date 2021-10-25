from generator import Generator
import pytest
import builtins
from unittest.mock import patch, mock_open, MagicMock
from math import ceil


@pytest.fixture
def gen():
    return Generator(4, 2, 3)


@pytest.fixture
def partitions(gen):
    return gen.make_partitions()


@pytest.fixture
def partitions_with_leaders(gen, partitions):
    return [x for x in gen.combine_partitions_with_leaders(partitions)]


@pytest.fixture
def testcases(gen, partitions_with_leaders):
    testcases = gen.combine_scenarios_with_rounds(
        partitions_with_leaders
    )
    return [x for x in testcases]


def test_make_generator_input_type_error():
    with pytest.raises(TypeError):
        _ = Generator(4, 2, 8, testcases_per_file='a')


def test_make_generator_input_value_error():
    with pytest.raises(ValueError):
        _ = Generator(4, 2, 8, machine_index=-1)


def test_make_generator_too_many_partitions_error():
    with pytest.raises(ValueError):
        _ = Generator(4, 20, 8)


def test_make_partitions():
    gen = Generator(4, 2, 8)
    partitions = gen.make_partitions()
    assert partitions == [
        [[0, 1, 2, 3], [4]],
        [[0, 1, 2, 4], [3]],
        [[0, 1, 3, 4], [2]],
        [[0, 2, 3, 4], [1]],
        [[0, 3, 4], [1, 2]],
        [[0, 1, 4], [2, 3]],
        [[0, 2, 4], [1, 3]],
        [[0, 4], [1, 2, 3]],
        [[0, 1, 2], [3, 4]],
        [[0, 1, 3], [2, 4]],
        [[0, 2, 3], [1, 4]],
        [[0, 3], [1, 2, 4]],
        [[0, 1], [2, 3, 4]],
        [[0, 2], [1, 3, 4]],
        [[0], [1, 2, 3, 4]],
    ]


def test_combine_partitions_with_leaders(gen, partitions):
    scenarios = gen.combine_partitions_with_leaders(partitions)
    length = len(partitions) * len(gen.target_nodes)
    assert len([s for s in scenarios]) == length


def test_combine_scenarios_with_rounds(gen, partitions_with_leaders):
    testcases = gen.combine_scenarios_with_rounds(partitions_with_leaders)
    length = len(partitions_with_leaders) ** gen.number_of_rounds
    assert len([x for x in testcases]) == length


def test_testcases_length(gen, testcases):
    assert len(testcases) == gen.testcases_length


def test_print_process(gen, testcases):
    with patch('builtins.open', mock_open()) as patcher:
        gen._print(testcases, 1, True, 1000)
        number_of_files = ceil(gen.testcases_length / 1000)
        assert patcher.call_count == number_of_files


def test_run(gen):
    gen.run(True, 1)


def test_run_type_input_type_error(gen):
    with pytest.raises(TypeError):
        gen.run(True, 'a')


def test_run_type_input_value_error(gen):
    with pytest.raises(ValueError):
        gen.run(True, 0)
