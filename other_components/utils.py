# Return the results of calling 'func' with '*args' while iterating over 'iterable' for the first 'func' argument
def iterate_func(func, iterable, *args):
    result = {}
    for value in iterable:
        dict = func(value, *args).popitem()
        result[dict[0]] = dict[1]
    return result