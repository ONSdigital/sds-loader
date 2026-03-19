import re


class SchemaFileParser:

    def parse(self, new_files: str) -> list[str]:
        return self._filter_files(self._message_to_list(new_files))

    def _filter_files(self, new_files: list[str]) -> list[str]:
        """
        Take a list of filepaths and match to regex of schemas/*/*.json.

        :param new_files: List of filepaths.
        :return: List of filepaths matching the regex.
        """

        pattern = re.compile(r"^schemas/[^/]+/[^/]+\.json$")
        return [f for f in new_files if pattern.match(f)]

    def _message_to_list(self, message: str) -> list[str]:
        """
        Convert a pub/sub message of filenames to a list of filenames. Message is \n delimited.

        :param message: The pub/sub message of filenames
        :return: The list of filenames
        """
        return message.split()
