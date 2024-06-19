class DataNotFoundException(Exception):
    pass


class RecordAlreadyExistsException(Exception):
    pass


class ThirdPartyServiceError(Exception):
    pass


class StatusUpdateError(Exception):
    pass


class ProjectModificationError(Exception):
    pass


class NotEnoughDataException(Exception):
    pass
