"""Test file 2 for the parallel upload test."""


def add_2(a, b):
    return a + b + 2


def multiply_2(a, b):
    return a * b * 2


if __name__ == "__main__":
    print(f"test2: add={add_2(1, 2)}, multiply={multiply_2(2, 3)}")