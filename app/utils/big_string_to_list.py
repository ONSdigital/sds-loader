

def big_string_to_list(big_string: str) -> list[str]:
    """
    Convert a single string with new-line delimited characters to a list.

    :param big_string: String to convert to a list.
    :return: List of strings.
    """
    return big_string.split()
