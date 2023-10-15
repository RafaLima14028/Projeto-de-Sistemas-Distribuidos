import re

ENCODING_AND_DECODING_TYPE = 'utf-8'


def check_string(text: str) -> bool:
    pattern = r"^(?!\s*$)(?!.*[,])"

    if re.match(pattern, text):
        return False
    else:
        return True  # There is an error


def check_number(number: str) -> int:
    if number != '':
        try:
            number = int(number)
        except ValueError:
            number = -1
    else:
        number = -1

    return number
