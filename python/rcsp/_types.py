"""Type markers used in node signatures: ``ts[T]`` and ``Outputs(...)``."""


class TsType:
    """The type produced by ``ts[T]`` — marks a time-series edge of element ``T``."""

    __slots__ = ("typ",)

    def __init__(self, typ):
        self.typ = typ

    def __repr__(self):
        return f"ts[{getattr(self.typ, '__name__', self.typ)}]"


class _Ts:
    """``ts`` singleton so that ``ts[int]`` yields a :class:`TsType`."""

    def __getitem__(self, typ):
        return TsType(typ)


ts = _Ts()


class Outputs:
    """Return-annotation marker for a node with multiple named outputs.

    Example::

        def f(x: ts[int]) -> Outputs(y=ts[int], z=ts[float]): ...
    """

    __slots__ = ("fields",)

    def __init__(self, **fields):
        # dict preserves insertion order → stable output indices
        self.fields = fields
