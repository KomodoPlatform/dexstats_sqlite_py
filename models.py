#!/usr/bin/env python3


class Pair():
    '''Allows for referencing pairs as a string or tuple.'''
    def __init__(self, pair):

        if isinstance(pair, (tuple)):
            self.as_tuple = pair
        elif isinstance(pair, (list)):
            self.as_tuple = tuple(pair)
        if isinstance(pair, str):
            self.as_tuple = tuple(map(str, pair.split('_')))

        if len(self.as_tuple) != 2:
            self.as_str = {"error": "not valid pair"}
            self.as_str = {"error": "not valid pair"}
            self.base = {"error": "not valid pair"}
            self.quote = {"error": "not valid pair"}
        else:
            self.as_str = pair[0] + "_" + pair[1]
            self.base = self.as_tuple[0]
            self.quote = self.as_tuple[1]
