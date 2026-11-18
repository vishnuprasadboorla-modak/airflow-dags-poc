"""Test file 10 for the parallel upload test."""


def add_10(a, b):
    return a + b + 10


def multiply_10(a, b):
    return a * b * 10


if __name__ == "__main__":
    print(f"test10: add={add_10(1, 2)}, multiply={multiply_10(2, 3)}")