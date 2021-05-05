class QueryParsingError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class DuplicateColumnDefError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class DuplicatePrimaryKeyDefError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class ReferenceTypeError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class ReferenceNonPrimaryKeyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class ReferenceColumnExistenceError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class ReferenceTableExistenceError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class NonExistingColumnDefError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class TableExistenceError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class DropReferencedTableError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class NoSuchTable(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class CharLengthError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


myErrors = [DuplicateColumnDefError, DuplicatePrimaryKeyDefError, ReferenceTypeError, ReferenceNonPrimaryKeyError,
            ReferenceColumnExistenceError, ReferenceTableExistenceError, NonExistingColumnDefError,
            TableExistenceError, DropReferencedTableError, NoSuchTable, CharLengthError]
