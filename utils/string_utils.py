import re
from unidecode import unidecode


def normalize_string(s: str) -> str:
    """
    Normalize strings removing accents and special characters.
    :param s: string to normalize
    :return: normalized string
    """

    s = unidecode(s)
    s = re.sub(r'\W+', ' ', s)

    s = s.strip()

    return s


def camel_to_snake(s: str) -> str:
    """
    Transform camel to snake case.
    :param s: string
    :return: string in camel case
    """
    s = re.sub(r'([A-Z]+)', r'_\1', s).lower()
    s = s.strip().strip('_')
    s = re.sub(r"_{2,}", "_", s)
    return s

