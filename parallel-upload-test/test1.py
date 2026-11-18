"""Test file 1 for the parallel upload test."""


def add_1(a, b):
    return a + b + 1


def multiply_1(a, b):
    return a * b * 1


if __name__ == "__main__":
    print(f"test1: add={add_1(1, 2)}, multiply={multiply_1(2, 3)}")