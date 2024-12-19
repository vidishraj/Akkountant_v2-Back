import datetime

from dateutil.relativedelta import relativedelta
from enums.DateFormatEnum import DateStatementEnum

datetime_formats = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    # "%m/%d/%Y %H:%M:%S", Not used in India, Can give a false positive
    # "%m/%d/%Y %H:%M", Not used in India, Can give a false positive
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d-%m-%y",
    # "%m/%d/%Y", Not used in India, Can give a false positive
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
    def find_matching_format(date_string):
        for fmt in datetime_formats:
            try:
                # Try parsing the date string with the format
                datetime.datetime.strptime(date_string, fmt)
                return fmt  # Return the matching format if successful
            except ValueError:
                continue  # Skip if the format doesn't match
        return None  # Return None if no matching format is found


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

    def convert_to_sql_datetime(self, date_str, bank):
        # Try format and return the correctly parsed datetime in SQL format
        try:
            parsed_date = datetime.datetime.strptime(date_str, getattr(DateStatementEnum, bank).value)
            return parsed_date.strftime("%Y-%m-%d %H:%M:%S")  # SQL datetime format
        except ValueError:
            possible_format = self.find_matching_format(date_str)
            if possible_format is not None:
                parsed_date = datetime.datetime.strptime(date_str, possible_format)
                return parsed_date.strftime("%Y-%m-%d %H:%M:%S")  # SQL datetime format
            raise ValueError(f"Date format of '{date_str}' is not recognized.")

    def convert_to_sql_datetime_date(self, date_str, bank):
        # Try each format and return the correctly parsed datetime in SQL format
        try:
            parsed_date = datetime.datetime.strptime(date_str, getattr(DateStatementEnum, bank).value)
            return parsed_date
        except ValueError:
            possible_format = self.find_matching_format(date_str)
            if possible_format is not None:
                parsed_date = datetime.datetime.strptime(date_str, possible_format)
                return parsed_date.strftime("%Y-%m-%d %H:%M:%S")  # SQL datetime format
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

    @staticmethod
    def convert_format_for_epf(date_str: str) -> str:
        """
        Converts a date from format '%d-%m-%Y' to '%Y-%m'.

        Args:
            date_str (str): Date string in '%d-%m-%Y' format.

        Returns:
            str: Date string in '%Y-%m' format.
        """
        try:
            parsed_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            return parsed_date.strftime("%Y-%m")
        except ValueError as e:
            raise ValueError(f"Error: Invalid date format. Expected '%d-%m-%Y', got '{date_str}'. {e}")

    @staticmethod
    def iterate_months(start_date: str):
        """
        Iterates from a given start date to the current month.

        Args:
            start_date (str): The starting date in '%Y-%m-%d' format.

        Yields:
            str: The current month in iteration in '%Y-%m' format.
        """
        try:
            start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            current = datetime.datetime.now()

            while start <= current:
                yield start.strftime("%Y-%m")
                start += relativedelta(months=1)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")