def is_even(x):
    return (x % 2) == 0

def upper(s):
    return s.upper()


def main():
    print(list(x for x in range(10) if is_even(x)))


if __name__ == '__main__':
    main()
