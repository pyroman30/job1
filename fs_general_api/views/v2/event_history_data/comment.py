from typing import Optional

from fs_general_api.views.v2.event_history_data import data_point


def generate_comment_event_data(comment: Optional[str]):
    if comment is None:
        return None

    return [
        data_point("Комментарий", comment),
    ]
