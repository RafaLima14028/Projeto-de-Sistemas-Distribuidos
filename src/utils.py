import re


def check_number(number: str) -> int:
    if number != '':
        try:
            number = int(number)
        except ValueError:
            number = -1
    else:
        number = -1

    return number


def check_string(text: str) -> bool:
    pattern = r"^(?!\s*$)(?!.*[,])"

    if re.match(pattern, text):
        return True
    else:
        return False
