CEND = '\33[0m'
CRED = '\33[31m'
CBLUE = '\33[34m'
CGREEN2 = '\33[92m'
CYELLOW2 = '\33[93m'


def print_info(*args):
    print(CGREEN2 + str(*args) + CEND)


def print_error(*args):
    print(CRED + str(*args) + CEND)


def print_warn(*args):
    print(CYELLOW2 + str(*args) + CEND)


def print_debug(*args):
    print(CBLUE + str(*args) + CEND)