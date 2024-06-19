from datetime import date

from fs_general_api.views.v2.event_history_data import data_point

date_format = "%d.%m.%Y"


def get_extra_data_for_retro_calculation_event(
    start_date: date, end_date: date
):
    return [
        data_point(
            "Диапазон дат",
            f"{start_date.strftime(date_format)} - {end_date.strftime(date_format)}",
        ),
    ]
