import re


class SchemaFileParser:

    def parse(self, big_string: str) -> list[str]:
        """
        Parse the big string to a list of valid filenames.
        :param big_string: The big string of newline separated filenames.
        """
        return self.filter_files(self._message_to_list(big_string))

    def filter_files(self, new_files: list[str]) -> list[str]:
        """
        Take a list of filepaths and match to regex of schemas/*/*.json.

        :param new_files: List of filepaths.
        :return: List of filepaths matching the regex.
        """

        pattern = re.compile(r"^schemas/[^/]+/[^/]+\.json$")
        return [f for f in new_files if pattern.match(f)]

    def _message_to_list(self, big_string: str) -> list[str]:
        """
        Convert a big string of newline separated filenames to a list of filenames

        :param big_string: A large string of newline separated filenames.
        :return: A list of the filenames separated by newlines.
        """
        return big_string.split()
