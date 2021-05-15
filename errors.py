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


class InsertReferentialIntegrityError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class InsertDuplicatePrimaryKeyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class InsertTypeMismatchError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class InsertColumnExistenceError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class InsertColumnNonNullableError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class DeleteReferentialIntegrityPassed(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class SelectTableExistenceError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class SelectColumnResolveError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class WhereIncomparableError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class WhereTableNotSpecified(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class WhereColumnNotExist(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class WhereAmbiguousReference(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


myErrors = [DuplicateColumnDefError, DuplicatePrimaryKeyDefError, ReferenceTypeError, ReferenceNonPrimaryKeyError,
            ReferenceColumnExistenceError, ReferenceTableExistenceError, NonExistingColumnDefError,
            TableExistenceError, DropReferencedTableError, NoSuchTable, CharLengthError,
            InsertReferentialIntegrityError, InsertDuplicatePrimaryKeyError, InsertTypeMismatchError,
            InsertColumnExistenceError, InsertColumnNonNullableError,
            DeleteReferentialIntegrityPassed, SelectTableExistenceError, SelectColumnResolveError,
            WhereIncomparableError, WhereTableNotSpecified, WhereColumnNotExist, WhereAmbiguousReference
            ]
