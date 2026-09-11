import ast
import inspect
import textwrap
from types import FunctionType

from pytest_check import check

from pyrio.decorators import is_terminal
from pyrio.iterators import ItertoolsMixin
from pyrio.streams import BaseStream, FileStream, Stream


### keep in-check @terminal decorated methods ###
def test_methods_marked_correctly_as_terminal_or_chaining():

    for cls in (BaseStream, ItertoolsMixin, Stream, FileStream):
        for name, obj in vars(cls).items():
            # skip private, dunder, class, static
            if name.startswith("_") or name == "close" or not isinstance(obj, FunctionType):
                continue

            with check:
                assert (terminal := is_terminal(obj)) ^ (chaining := _returns_self(obj)), (
                    f"{cls.__name__}.{name}: cannot be {terminal=} and {chaining=}"
                )


def _returns_self(func):

    # NB: getsource keeps class-body indent -> dedent so ast.parse accepts it
    src = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(src)
    return any(
        # check if node is a 'return' with value (bare name) 'self'
        # i.e. Return(value=Name(id='self', ctx=Load()))
        isinstance(n, ast.Return) and isinstance(n.value, ast.Name) and n.value.id == "self"
        for n in ast.walk(tree)
    )
