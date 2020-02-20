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
    def __init__(self, number, books, signup, ship, scores):
        self.number: int = number
        self.books: List[int] = books
        self.signup_time: int = signup
        self.nb_ship: int = ship
        self.book_scores: List[int] = [sum(scores[book] for book in books[:i]) for i in range(1, len(books))]

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
        libraries.append(Library(lib, books, signup, ship, scores))

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


    def recurse(libs: List[Library], rest_days: int):
        first_no = first(libs, lambda l: l.signup_time >= rest_days)
        if first_no is not None:
            libs = libs[:first_no]

        for i, lib in enumerate(libs):
            rest_day = rest_days - lib.signup_time
            nb_books = min(rest_day * lib.nb_ship, len(lib.books))

            books_sent = lib.books[:nb_books]
            cur_score = lib.book_scores[nb_books - 1]
            output_libs.add_library(lib.number + i, books_sent)
            save(cur_score)
            recurse(libs[i+1:], rest_days - lib.signup_time)
            output_libs.pop()

    def recurse1(libs: List[Library], rest_days: int):
        first_no = first(libs, lambda l: l.signup_time >= rest_days)
        if first_no is not None:
            libs = libs[:first_no]

        with ProcessPoolExecutor() as exe:
            fut = []
            for i, lib in enumerate(libs):
                rest_day = rest_days - lib.signup_time
                nb_books = min(rest_day * lib.nb_ship, len(lib.books))

                books_sent = lib.books[:nb_books]
                cur_score = lib.book_scores[nb_books - 1]
                output_libs.add_library(lib.number + i, books_sent)
                save(cur_score)
                fut.append(exe.submit(recurse, libs[i+1:], rest_days - lib.signup_time))
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

    return computeAntoine(input_data, args)


def compute_lib_score_and_book_list(lib, days_left, available_books, scores):
    if (lib.signup_time >= days_left):
        return 0, []
    days_left -= lib.signup_time
    final_score = 0
    sorted_books = sort_books(scores, lib.books)

    #remove already shiped books
    for book_index in range(len(sorted_books)):
        if not available_books[sorted_books[book_index]]:
            sorted_books.pop(book_index)
            book_index -= 1

    #compute score
    nb_books = 0
    for i in range(days_left):
        for j in range(lib.nb_ship):
            if (nb_books >= len(sorted_books)):
                break
            final_score += scores[nb_books]
            nb_books += 1

    return (final_score, sorted_books[:nb_books + 1])

def sort_books(scores, books):
    sorted_books = []
    while len(books) > 0:
        max_score_book_index = 0
        for i in range(len(books)):
            if scores[books[max_score_book_index]] < scores[books[i]]:
                max_score_book_index = i
        sorted_books.append(books[max_score_book_index])
        books.pop(max_score_book_index)
    return sorted_books


def computeAntoine(input_data: ComputeInput, args: CLIArgs) -> ComputeOutput:
    libraries, scores, nb_days = input_data
    day_counter = 0
    available_books = [True for _ in range(len(scores))]
    output = OutputLibs()
    total_score = 0
    while len(libraries) > 0 and nb_days > day_counter:
        best_score, best_book_list = compute_lib_score_and_book_list(libraries[0], nb_days - day_counter, available_books, scores)
        best_lib = 0
        ind = 0
        for lib in libraries:
            score, book_list = compute_lib_score_and_book_list(lib, nb_days - day_counter, available_books, scores)
            if score > best_score:
                best_score = score
                best_book_list = book_list
                best_lib = ind
            ind += 1
        output.add_library(best_lib, best_book_list)
        for i in best_book_list:
            available_books[i] = False
        day_counter += libraries[best_lib].signup_time
        libraries.pop(best_lib)
        total_score += best_score
    print(total_score)
    return str(output)


if __name__ == '__main__':
    args = parse_arguments()

    input_data = parse_input(args.input_file)

    output_data = compute(input_data, args)

    save_output(args.output_file, output_data)
