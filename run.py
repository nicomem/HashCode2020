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
    def __init__(self, number, books, signup, ship):
        self.number: int = number
        self.books: List[int] = books
        self.signup_time: int = signup
        self.nb_ship: int = ship

    def __repr__(self):
        return f'{" ".join(map(str, self.books))}-{self.signup_time}-{self.nb_ship}'


class OutputLibs:
    def __init__(self):
        self.libs = []

    def add_library(self, lib: int, books_sent: List[int]):
        self.libs.append((lib, books_sent))

    def pop(self):
        self.libs.pop()

    def __repr__(self):
        r = f'{len(self.libs)}\n'
        for lib in self.libs:
            r += f'{lib[0]} {len(lib[1])}\n'
            r += ' '.join(map(str, lib[1])) + '\n'
        return r

    def __str__(self):
        return self.__repr__()


# Type definitions
ComputeOutput = str
ComputeInput = (List[Library], List[int], int)

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
    scores = list(map(int, lines[1].strip().split(' ')))

    libraries = []
    for lib in range(nb_libraries):
        _, signup, ship = map(int, lines[2 + 2 * lib].strip().split(' '))
        books = list(map(int, lines[3 + 2 * lib].strip().split(' ')))
        books.sort(key=lambda i: -scores[i])
        libraries.append(Library(lib, books, signup, ship))

    return (libraries, scores, nb_days)


def save_output(file: Path, data: str) -> None:
    # Make sure the parent directory exists
    file.parent.mkdir(parents=True, exist_ok=True)

    # Create or override the file with the data
    file.write_text(data)


    # nb_libs
    # for each lib done:
    #   lib_no nb_books
    #   ' '.join(id_books)

def first(l: list, cond):
    for i, e in enumerate(l):
        if cond(e):
            return i

    return None

def brute_force(input_data: ComputeInput, args) -> ComputeOutput:
    libraries, scores, nb_days = input_data
    output_libs = OutputLibs()
    max_score = 0
    max_res_str = ''

    libraries.sort(key=lambda l: l.signup_time)

    def save(score: int):
        nonlocal max_score
        if score <= max_score:
            return

        print(f'saving: score {score}')
        max_score = score
        max_res_str = str(output_libs)
        save_output(args.output_file.with_suffix(f'.score_{score}'), max_res_str)

    def recurse(libs: List[Library], rest_days: int, offset: int = 0):
        first_no = first(libs, lambda l: l.signup_time >= rest_days)
        if first_no is not None:
            libs = libs[:first_no]

        for i, lib in enumerate(libs):
            rest_day = rest_days - lib.signup_time
            nb_books = min(rest_day * lib.nb_ship, len(lib.books))

            books_sent = lib.books[:nb_books]
            cur_score = sum(scores[book] for book in books_sent)
            output_libs.add_library(offset + i, books_sent)
            save(cur_score)
            recurse(libs[i+1:], rest_days - lib.signup_time, offset + i + 1)
            output_libs.pop()

    def recurse1(libs: List[Library], rest_days: int, offset: int = 0):
        first_no = first(libs, lambda l: l.signup_time >= rest_days)
        if first_no is not None:
            libs = libs[:first_no]

        with ProcessPoolExecutor() as exe:
            fut = []
            for i, lib in enumerate(libs):
                rest_day = rest_days - lib.signup_time
                nb_books = min(rest_day * lib.nb_ship, len(lib.books))

                books_sent = lib.books[:nb_books]
                cur_score = sum(scores[book] for book in books_sent)
                output_libs.add_library(offset + i, books_sent)
                save(cur_score)
                fut.append(exe.submit(recurse, libs[i+1:], rest_days - lib.signup_time, offset + i + 1))
                output_libs.pop()

            for _ in tqdm(as_completed(fut)):
                pass


    recurse1(libraries, nb_days)

    return max_res_str

def compute(input_data: ComputeInput, args: CLIArgs) -> ComputeOutput:
    # TODO: Compute things
    # Possibly useful classes/functions:
    # - {Thread/Process}PoolExecutor: async in threads/processes
    # - Pool: Process pool for concurrent usage
    # - tqdm: Progress bar
    # - np: Numpy for optimizations

    return brute_force(input_data, args)


if __name__ == '__main__':
    args = parse_arguments()

    input_data = parse_input(args.input_file)

    output_data = compute(input_data, args)

    # save_output(args.output_file, output_data)
