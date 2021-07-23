from typing import List, Dict, Tuple, Iterator
import logging
import json
import csv

from scc_class import SCCProb


class InputMetadata:
    input_directory: str
    input_prefix: str
    suffix_digits: int  # digits of the number at the end
    input_index_list: List[int]

    mc_env_suffix: str
    mc_env_extension: str
    cast_suffix: str
    cast_extension: str
    duedate_suffix: str
    duedate_extension: str
    processtime_suffix: str
    processtime_extension: str
    processtime_header: List[str]
    i_encoding: str

    # input problem size
    cast_lth_min: int
    cast_lth_max: int
    limit_by_casts: bool
    cast_count_min: int
    cast_count_max: int
    limit_by_charges: bool
    charge_count_min: int
    charge_count_max: int

    # IGM algorithm time limits
    short_ttl: int
    long_ttl: int
    ih_cast_timelimit: int  # for an addition of a cast in cast iteration
    ih_termination_gap_increment: float
    dca_repeat: int  # for cast DC
    dca_timelimit: int  # for cast DC
    dca_continue_diff: float
    dch_window_minutes: int  # for charge DC
    dch_step_minutes: int  # for charge DC
    dch_timelimit: int  # for charge DC

    total_timelimit: int

    def __init__(self, filename: str, encoding: str):
        self.fill_from_json(filename, encoding)
        self.define_idx_format()

    def fill_from_json(self, filename: str, encoding: str):
        with open(filename, encoding=encoding) as _file:
            input_dict = json.load(_file)
            for key in input_dict.keys():
                self.__dict__[key] = input_dict[key]

    def __str__(self):
        _str = "\n" + self.__class__.__name__ + " contents -- "
        for key, value in self.__dict__.items():
            _str += f"{key}: {value}\t"
        return _str

    def check_input_reading(self):
        """checks if all input files are okay

        Raises:
            OSError
                If error reading processing time file
                If error reading duedate time file
        """
        for idx in self.input_index_list:
            mc_env_fn, cast_fn, dd_fn, pt_fn = self.a_file_location_set(idx)
            try:
                self.read_mc_env(mc_env_fn)
            except FileNotFoundError:
                raise OSError(f"Reading file {mc_env_fn} incurs error")
            try:
                self.read_cast(cast_fn)
            except FileNotFoundError:
                raise OSError(f"Reading file {cast_fn} incurs error")
            try:
                self.read_duedate(dd_fn)
            except FileNotFoundError:
                raise OSError(f"Reading file {dd_fn} incurs error")
            try:
                self.read_processtime(pt_fn)
            except FileNotFoundError:
                raise OSError(f"Reading file {pt_fn} incurs error")

        _str = f"\nReading all {len(self.input_index_list)} input files is OK"
        logging.info(_str)

    def check_prob_size_params(self):
        """check if problem size parameters are okay for random generations

        for problem_set_generator.py

        Raises:
            ValueError: more than one limiting policy is selected
            ValueError: a parameter for selected limiting policy is undefined
        """
        _iserr = False
        _str = ""
        if (self.limit_by_casts and self.limit_by_charges) or not (
            self.limit_by_casts or self.limit_by_charges
        ):
            _str = "Choose one among two limiting policies: casts or charges"
            raise ValueError(_str)
        elif self.limit_by_casts:
            if "cast_count_max" not in self.__dict__:
                _str += "cast_count_max "
                _iserr = True
            if "cast_count_min" not in self.__dict__:
                _str += "cast_count_min "
                _iserr = True
        elif self.limit_by_charges:
            if "charge_count_min" not in self.__dict__:
                _str = "charge_count_min "
                _iserr = True
            if "charge_count_max" not in self.__dict__:
                _str = "charge_count_max "
                _iserr = True

        if _iserr:
            _str += "not defined in input_metadata.json"
            raise ValueError(_str)

    def prob_name(self, idx: int) -> str:
        return self.input_prefix + "_" + str(idx).zfill(self.suffix_digits)

    def iterate_prob_ins(self) -> Iterator[SCCProb]:
        """
        Yields:
            Iterator[SCCProb]
                generating problem instance according to self.input_index_list
        """
        # maximum digit of problem instances
        for idx in self.input_index_list:
            scc_prob = SCCProb(self.prob_name(idx))
            mc_env_fn, cast_fn, dd_fn, pt_fn = self.a_file_location_set(idx)
            stage_seq, stage_mc_dict = self.read_mc_env(mc_env_fn)
            cast_seq, ca_ch_dict = self.read_cast(cast_fn)
            dd_dict = self.read_duedate(dd_fn)
            pt_dict = self.read_processtime(pt_fn)

            scc_prob.stage_list = stage_seq
            scc_prob.stage_mc_id_dict = stage_mc_dict
            scc_prob.ca_id_list = cast_seq
            scc_prob.ca_ch_id_dict = ca_ch_dict
            scc_prob.ch_duedate_dict = dd_dict
            scc_prob.ch_mc_id_processtime_dict = pt_dict
            scc_prob.ch_stage_dict = self.compose_ch_stage_dict(
                pt_dict, stage_seq, stage_mc_dict
            )

            yield scc_prob

    def a_file_location_set(self, idx: int) -> Tuple[str, str, str, str]:
        """
        Args:
            idx (int): index of the input data file pair

        Returns:
            Tuple[str, str, str, str]
                machine environment filename
                cast filename
                due date filename
                processing time filename
        """
        digits = self.idx_format
        prefix = self.input_directory + self.input_prefix + format(idx, digits)
        mc_env_fn = prefix + self.mc_env_suffix + self.mc_env_extension
        cast_fn = prefix + self.cast_suffix + self.cast_extension
        dd_fn = prefix + self.duedate_suffix + self.duedate_extension
        pt_fn = prefix + self.processtime_suffix + self.processtime_extension
        return mc_env_fn, cast_fn, dd_fn, pt_fn

    def define_idx_format(self):
        self.idx_format: str = "0" + str(self.suffix_digits)

    def path_prefix(
        self, location: str, ins_idx: int, filename_prefix: str = ""
    ) -> str:
        return (
            f"{location}/{filename_prefix}{format(ins_idx, self.idx_format)}"
        )

    def read_mc_env(
        self, mc_env_location: str
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        """read machine environment data

        Args:
            mc_env_location (str)

        Raises:
            ValueError: if stage sequence is not defined

        Returns:
            Tuple[List[str], Dict[str, List[str]]]
                stage ID sequence
                stage ID -> list of machine IDs
        """
        stage_seq: List[str] = list()
        stage_mc_dict: Dict[str, List[str]] = dict()

        with open(mc_env_location, encoding=self.i_encoding) as _f:
            input_dict = json.load(_f)
            for key in input_dict.keys():
                if key == "stage_seq":
                    stage_seq = input_dict[key]
                else:
                    stage_mc_dict[key] = input_dict[key]

        if not stage_seq:
            raise ValueError("Stage sequence not defined in", mc_env_location)
        return stage_seq, stage_mc_dict

    def read_cast(
        self, cast_location: str
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        """read machine environment data

        Args:
            cast_location (str)

        Raises:
            ValueError: if stage sequence is not defined

        Returns:
            Tuple[List[str], Dict[str, List[str]]]
                cast ID sequence
                cast ID -> list of charge IDs
        """
        cast_seq: List[str] = list()
        ca_ch_dict: Dict[str, List[str]] = dict()

        with open(cast_location, encoding=self.i_encoding) as _f:
            input_dict = json.load(_f)
            for key in input_dict.keys():
                if key == "cast_seq":
                    cast_seq = input_dict[key]
                else:
                    ca_ch_dict[key] = input_dict[key]

        if not cast_seq:
            raise ValueError("Cast sequence not defined in", cast_location)
        return cast_seq, ca_ch_dict

    def read_duedate(self, dd_location: str) -> Dict[str, int]:
        """read duedate info

        Args:
            dd_location (str)

        Returns:
            Dict[str, int]: charge ID -> due date
        """
        with open(dd_location, encoding=self.i_encoding) as _f:
            dd_dict = json.load(_f)
        return dd_dict

    def read_processtime(self, pt_location: str) -> Dict[str, Dict[str, int]]:
        """read process time info

        Args:
            pt_location (str)

        Returns:
            Dict[str, Dict[str, int]]: charge ID -> machine ID -> pt
        """
        pt_dict: Dict[str, Dict[str, int]] = dict()

        with open(
            pt_location, "r", newline="", encoding=self.i_encoding
        ) as _f:
            pt_reader = csv.DictReader(_f)
            for row in pt_reader:
                ch_id, mc_id, pt = row["ch_id"], row["mc_id"], int(row["pt"])
                if ch_id not in pt_dict:
                    pt_dict[ch_id] = dict()
                pt_dict[ch_id][mc_id] = pt

        return pt_dict

    @staticmethod
    def compose_ch_stage_dict(
        pt_dict: Dict[str, Dict[str, int]],
        stage_seq: List[str],
        stage_mc_dict: Dict[str, List[str]],
    ) -> Dict[str, List[str]]:
        ch_stage_dict: Dict[str, List[str]] = {
            ch_id: list() for ch_id in pt_dict.keys()
        }

        for stage_id in stage_seq:
            stage_mcs = stage_mc_dict[stage_id]
            for ch_id, mc_dict in pt_dict.items():
                for mc_id in mc_dict:
                    if mc_id in stage_mcs:
                        ch_stage_dict[ch_id].append(stage_id)
                        break

        return ch_stage_dict
