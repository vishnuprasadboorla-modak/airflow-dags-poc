"""Test file 5 for the parallel upload test."""


def add_5(a, b):
    return a + b + 5


def multiply_5(a, b):
    return a * b * 5


if __name__ == "__main__":
    print(f"test5: add={add_5(1, 2)}, multiply={multiply_5(2, 3)}")