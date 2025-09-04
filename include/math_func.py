def get_fibonacci_sequence(num) -> list[int]:
    fib_sequence = []
    a, b = 0, 1
    for _ in range(num):
        a, b = b, a + b

        fib_sequence.append(a)

    return fib_sequence