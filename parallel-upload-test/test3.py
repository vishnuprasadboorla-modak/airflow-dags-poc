"""Test file 3 for the parallel upload test."""


def add_3(a, b):
    return a + b + 3


def multiply_3(a, b):
    return a * b * 3


if __name__ == "__main__":
    print(f"test3: add={add_3(1, 2)}, multiply={multiply_3(2, 3)}")