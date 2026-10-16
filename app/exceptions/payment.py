class PaymentNotFoundError(Exception):
    pass


class IdempotencyConflictError(Exception):
    pass


class PaymentStorageError(Exception):
    pass
