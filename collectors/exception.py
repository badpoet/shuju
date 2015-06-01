__author__ = 'badpoet'


class WrongPage(Exception):

    def __str__(self):
        return "Not the correct page."

