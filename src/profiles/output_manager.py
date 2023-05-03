import copy

from src.profiles import profile_consts as pc
from src.profiles import profile_helpers as ph


class OutputManager:
    def __init__(
            self
            ) -> None:
        self.output_dict = self._create_output_dict()
        self.dirs_to_ignore = []
        self.files_to_ignore = []


    def add_success_entry_to_dict(
            self,
            value: str,
            process: str,
            note: str,
            ) -> None:
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_SUCCESS_KEY].append(entry_subdict)


    def add_ignored_entry_to_dict(
            self,
            value: str,
            process: str,
            note: str,
            ) -> None:
        entry_subdict = self.create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_ISSUES_KEY].append(entry_subdict)


    def add_issues_entry_to_dict(
            self,
            value: str,
            process: str,
            note: str,
            ) -> None:
        entry_subdict = self._create_output_subdict()
        entry_subdict[pc.OUTPUT_VALUE_SUBKEY] = value
        entry_subdict[pc.OUTPUT_PROCESS_SUBKEY] = process
        entry_subdict[pc.OUTPUT_NOTES_SUBKEY] = note
        self.output_dict[pc.OUTPUT_ISSUES_KEY].append(entry_subdict)


    def add_dir_to_ignore(self,
                          dir: str,
                          ) -> None:
        self.dirs_to_ignore.append(dir)


    def add_file_to_ignore(self,
                           fp: str,
                           ) -> None:
        self.files_to_ignore.append(fp)


    def add_files_to_ignore(self,
                            files: list[str],
                            ) -> None:
        self.files_to_ignore.extend(files)


    def _create_output_dict(
            self,
            ) -> dict:
        out_dict = ph.create_output_dict()
        for key in out_dict.keys():
            out_dict[key] = []
        return out_dict


    def _create_output_subdict(
            self,
            ) -> dict:
        return ph.create_output_subdict()