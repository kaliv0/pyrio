from pyrio.exception import NoSuchElementError, NullPointerError


class Optional:
    def __init__(self, element):
        self._element = element

    def __str__(self):
        return f"Optional[{self._element}]"

    @staticmethod
    def empty():
        return Optional(None)

    @staticmethod
    def of(element):
        if element is None:
            raise NullPointerError("Value cannot be None")
        return Optional(element)

    @staticmethod
    def of_nullable(element):
        return Optional(element)

    def get(self):
        if self.is_empty():
            raise NoSuchElementError("Optional is empty")
        return self._element

    def is_present(self):
        return not self.is_empty()

    def is_empty(self):
        return self._element is None

    def if_present(self, action):
        if self.is_present():
            action(self.get())

    def if_present_or_else(self, action, empty_action):
        if self.is_present():
            action(self.get())
        else:
            empty_action()

    def or_else(self, value):
        return self._element if self.is_present() else value

    def or_else_get(self, supplier):
        return self._element if self.is_present() else supplier()
