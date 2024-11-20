import datetime

from enums.DateFormatEnum import DateStatementEnum

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
    "%b %d, %Y",
    "%d/%m/%Y",
    "%d/%m/%y"
]


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
    def convert_to_sql_datetime(date_str, bank):
        # Try format and return the correctly parsed datetime in SQL format
        try:
            parsed_date = datetime.datetime.strptime(date_str, getattr(DateStatementEnum, bank).value)
            return parsed_date.strftime("%Y-%m-%d %H:%M:%S")  # SQL datetime format
        except ValueError:
            raise ValueError(f"Date format of '{date_str}' is not recognized.")

    @staticmethod
    def convert_to_sql_datetime_date(date_str, bank):
        # Try each format and return the correctly parsed datetime in SQL format

        try:
            parsed_date = datetime.datetime.strptime(date_str, getattr(DateStatementEnum, bank).value)
            return parsed_date
        except ValueError:
            raise ValueError(f"Date format of '{date_str}' is not recognized.")

    def getMonthYearRange(self, date1, date2, bank) -> str:
        # Convert to dateTime first
        date1: datetime = self.convert_to_sql_datetime_date(date1, bank)
        date2: datetime = self.convert_to_sql_datetime_date(date2, bank)

        # Casual check to make sure its sorted dates
        if date1 > date2:
            date1, date2 = date2, date1

        # Format each date as "Month-Year"
        month_year1 = date1.strftime("%B-%Y")
        month_year2 = date2.strftime("%B-%Y")

        # If both dates are in the same month and year, return a single formatted string
        if month_year1 == month_year2:
            return month_year1
        # If they differ, return the range format
        else:
            return f"{month_year1}_{month_year2}"

    @staticmethod
    def getCurrentDatetimeSqlFormat():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
