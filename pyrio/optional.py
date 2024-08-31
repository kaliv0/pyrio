from pyrio.exception import NoSuchElementError, NullPointerError


class Optional:
    def __init__(self, element):
        self._element = element

    @staticmethod
    def empty():
        return Optional(None)

    @staticmethod
    def of(element):
        if element is None:
            raise NullPointerError("Optional is empty")  # TODO: change message
        return Optional(element)

    @staticmethod
    def of_nullable(element):
        return Optional(element)

    def get(self):
        if self.is_empty():
            raise NoSuchElementError(
                "Optional is empty"
            )  # TODO: should we raise when getting from empty optional?
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
            action(self.get())  # TODO: do we need return here?
        else:
            empty_action()

    def or_else(self, value):
        return self._element if self.is_present() else value

    def or_else_get(self, supplier):
        return self._element if self.is_present() else supplier()

    def filter(self, predicate):
        if self.is_present() and predicate(self.get()):
            return self
        return Optional.empty()

    def map(self, mapper):
        if self.is_present():
            return Optional.of_nullable(mapper(self.get()))
        return Optional.empty()
