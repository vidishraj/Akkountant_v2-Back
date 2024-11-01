import datetime


class DateTimeUtil:

    @staticmethod
    def currentMonthDatesForEmail():
        current_date = datetime.date.today()
        first_day = current_date.replace(day=1)

        if current_date.month == 12:
            last_day = current_date.replace(year=current_date.year + 1, month=1, day=1) - datetime.timedelta(days=1)
        else:
            last_day = current_date.replace(month=current_date.month + 1, day=1) - datetime.timedelta(days=1)

        first_day_formatted = first_day.strftime("%Y/%-m/%-d")
        last_day_formatted = last_day.strftime("%Y/%-m/%-d")
        return first_day_formatted, last_day_formatted

    @staticmethod
    def convert_to_sql_datetime(date_str):
        # Define a list of common datetime formats
        datetime_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d-%m-%Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%m/%d/%Y",
            "%d %b %Y %H:%M:%S",
            "%d %B %Y %H:%M:%S",
            "%d %b %Y",
            "%d %B %Y",
            "%d-%m-%Y",
            "%d-%m-%y",
            "%b %d, %Y"
        ]

        # Try each format and return the correctly parsed datetime in SQL format
        for fmt in datetime_formats:
            try:
                parsed_date = datetime.datetime.strptime(date_str, fmt)
                return parsed_date.strftime("%Y-%m-%d %H:%M:%S")  # SQL datetime format
            except ValueError:
                continue  # Try the next format

        raise ValueError(f"Date format of '{date_str}' is not recognized.")