#!/usr/bin/env python3

# Std lib
import argparse
from pathlib import Path
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    Executor,
    as_completed
)
from multiprocessing import Pool
from typing import List


# Other libs
import numpy as np
from tqdm import tqdm

class CLIArgs:
    def __init__(self, args):
        self.input_file: Path = args.input
        self.output_file: Path = args.output


class Library:
    def __init__(self, books, signup, ship):
        self.books: List[int] = books
        self.signup_time: int = signup
        self.nb_ship: int = ship

    def __repr__(self):
        return f'{" ".join(map(str, self.books))}-{self.signup_time}-{self.nb_ship}'

# Type definitions
ComputeOutput = str
ComputeInput = (List[Library], List[int])

def parse_arguments() -> CLIArgs:
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input', type=Path, required=True)
    parser.add_argument('-o', '--output', type=Path, default=None)

    args = parser.parse_args()

    if args.output is None:
        args.output = Path.cwd() / 'out' / args.input.name

    return CLIArgs(args)


def parse_input(file: Path) -> ComputeInput:
    data = file.read_text()
    lines = data.split('\n')

    nb_books, nb_libraries, nb_days = map(int, lines[0].strip().split(' '))
    scores = map(int, lines[1].strip().split(' '))

    libraries = []
    for lib in range(nb_libraries):
        _, signup, ship = map(int, lines[2 + 2 * lib].strip().split(' '))
        books = map(int, lines[3 + 2 * lib].strip().split(' '))
        libraries.append(Library(books, signup, ship))

    print(libraries)

    return (libraries, scores)


def save_output(file: Path, data: str) -> None:
    # Make sure the parent directory exists
    file.parent.mkdir(parents=True, exist_ok=True)

    # Create or override the file with the data
    file.write_text(data)


def compute(input_data: ComputeInput, args: CLIArgs) -> ComputeOutput:
    # TODO: Compute things
    # Possibly useful classes/functions:
    # - {Thread/Process}PoolExecutor: async in threads/processes
    # - Pool: Process pool for concurrent usage
    # - tqdm: Progress bar
    # - np: Numpy for optimizations

    return input_data


if __name__ == '__main__':
    args = parse_arguments()

    input_data = parse_input(args.input_file)

    output_data = compute(input_data, args)

    save_output(args.output_file, output_data)
