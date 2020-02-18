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

# Other libs
import numpy as np
from tqdm import tqdm


# Type definitions
ComputeOutput = str
ComputeInput = str


class CLIArgs:
    def __init__(self, args):
        self.input_file: Path = args.input
        self.output_file: Path = args.output


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

    # TODO: Parse file data in a meaningful way

    return data


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
