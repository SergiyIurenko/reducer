import logging
import pandas as pd
import os
import multiprocessing as mp
import logging as lg
import socket
from typing import Generator


def get_logger(name: str, level: str) -> lg.Logger:
    """Returns formatted logger labeled by 'name' and limited by 'level'.

       Please refine this function to fit your logging infrastructure e.g. add handler from pygelf to feed Graylog.
    """
    logger = lg.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s:   %(message)s"))
        logger.addHandler(handler)
    return logger


def is_valid_ip(addr: str) -> bool:
    """Checks via Sockets lib if IP address is valid"""
    try:
        socket.inet_aton(addr)
        return True
    except socket.error:
        return False


def sorting_key(ip: str) -> int:
    """Converts a.b.c.d formatted IP address to 4-byte int to be used as a sorting key"""
    segments = ip.split(".")
    return int(segments[3]) + 256 * int(segments[2]) + 65536 * int(segments[1]) + 16777216 * int(segments[0])


def compact_data(dataset: pd.DataFrame) -> pd.DataFrame:
    """Aggregates rows in dataset.

       Please adjust this function if you want different way of aggregation, e.g. count. You may need more
       columns in dataset for your aggregation function.
    """
    return dataset.drop_duplicates(ignore_index=True)


class Runner(mp.Process):
    """Runner class to be instantiated in Runner subprocesses to perform data reduction workload.

       To customize behaviour please inherit your own class from it and override methods as needed.
    """

    def __init__(self, config: dict, runner_id: int) -> None:
        """Prepares runner object

           Please override if you have changed aggregation method to get proper self.compactor
        """
        super().__init__()
        self.cfg = config
        self.runner_id = runner_id
        self.compactor = compact_data
        self.logger = get_logger("Runner [" + str(runner_id + 1) + "]", cfg["log_level"])
        self.logger.debug("Runner initialized")

    def is_valid_file(self, file: str) -> bool:
        """Determines if file name looks like the file we have to process: 'aaa bbb.csv' or 'aaa bbb ccc.csv'

           :param file: str - file name to look at
        """
        parts = file.split(".")
        if len(parts) == 2:
            if parts[-1] == "csv":
                parts = parts[0].split(" ")
                if len(parts) == 2 or len(parts) == 3:
                    return True
        return False

    def file_enumerator(self) -> Generator[int, None, None]:
        """Generator yielding file names to process by this specific runner. File names are taken from 'input_dir'
           recursive walk-through, from files with valid names it yields by index [runner_id, runner_id +
           1 x runners_qty, runner_id + 2 x runners_qty, ...]

           :return: Generator of str

           Please override this method if you source data is not in host FS. E.g. you may want to do that to
           enumerate files stored in Hadoop or SFTP server.
        """
        n = 0
        for folder, _, files in os.walk(self.cfg["input_dir"], topdown=True):
            for file in files:
                if self.is_valid_file(file):
                    if (n % self.cfg["runners_qty"]) == self.runner_id:
                        self.logger.debug("File peeked for processing: " + folder + os.sep + file)
                        yield folder + os.sep + file
                    else:
                        self.logger.debug("File of another runner: " + folder + os.sep + file)
                    n += 1
                else:
                    self.logger.debug("File ignored: " + folder + os.sep + file)

    def env_from_file(self, file: str) -> str:
        """Extracts first two elements from file name to be used in 'Environment' column

           :param file: str - file name

           :return: str - Environment value
        """
        return " ".join(file.split(os.sep)[-1].split(".csv")[0].split(" ")[:2])

    def data_reader(self) -> Generator[pd.DataFrame, None, None]:
        """Generator yielding data batches from files selected for processing on this runner

           :return: pandas DataFrame

           Please override this method if you source data is not in host FS. E.g. you may want to do that to
           first read file stored in Hadoop or SFTP server and then chunk it into data batches.
        """
        for file in self.file_enumerator():
            self.logger.debug("Processing file " + file)
            try:
                for chunk in pd.read_csv(file, usecols=["Source IP"], na_filter=False,
                                         dtype={"Source IP": str}, engine="c",
                                         chunksize=self.cfg["batch_size"],
                                         on_bad_lines="skip"):
                    chunk = chunk[chunk["Source IP"].apply(is_valid_ip)]
                    chunk["Environment"] = self.env_from_file(file)
                    yield chunk
            except Exception as e:
                self.logger.warning("File " + file + " ignored due to error: " + str(e))

    def run(self) -> None:
        """Entrypoint for subprocess execution

           Reads one-by-one data chunks from generator, adds to result and compacts that as needed
        """
        self.logger = get_logger("Runner [" + str(self.runner_id + 1) + "]", self.cfg["log_level"])
        self.logger.debug("Runner started")
        result = None
        for data_chunk in self.data_reader():
            if result is None:
                result = data_chunk
            else:
                result = pd.concat([result, data_chunk], ignore_index=True)
            if len(result.index) > self.cfg["compact_at"]:
                result = self.compactor(result)
        if result is not None:
            result = self.compactor(result)
        self.save_result(result)
        self.logger.debug("Runner complete")

    def save_result(self, dataset: pd.DataFrame) -> None:
        """Stores partial result of this specific runner

           :param dataset: pandas DataFrame - partial result dataset

           Please override this method if you result data is not in host FS. E.g. you may want to do that to
           save to Hadoop or SFTP server.
        """
        result_name = self.cfg["output_dir"] + os.sep + ".Combined_" + str(self.runner_id) + ".csv"
        if dataset is None:
            self.logger.debug("Result is empty")
            if os.path.isfile(result_name):
                os.unlink(result_name)
        else:
            self.logger.debug("Saving result")
            dataset.to_csv(self.cfg["output_dir"] + os.sep + ".Combined_" + str(self.runner_id) + ".csv", index=False,
                           line_terminator="\n")


if __name__ == "__main__":
    # Read configuration from environment. Use defaults for missing elements
    cfg = {
        "input_dir": os.getenv("REDUCER_INPUT_DIR", "data"),
        "output_dir": os.getenv("REDUCER_OUTPUT_DIR", "data"),
        "runners_qty": int(os.getenv("REDUCER_RUNNERS_QTY", 1)),
        "low_runner": int(os.getenv("REDUCER_LOW_RUNNER", 1)),
        "high_runner": int(os.getenv("REDUCER_HIGH_RUNNER", 1)),
        "batch_size": int(os.getenv("REDUCER_BATCH_SIZE", 250000)),
        "compact_at": int(os.getenv("REDUCER_COMPACT_AT", 1000000)),
        "mode": os.getenv("REDUCER_MODE", "reduce_collect"),
        "log_level": os.getenv("LOG_LEVEL", "INFO")
    }

    logger = get_logger("Coordinator [" + str(cfg["low_runner"]) + ".." + str(cfg["high_runner"]) + "]",
                        cfg["log_level"])
    logger.debug("Config = " + str(cfg))

    # Initial checks
    if not os.path.isdir(cfg["output_dir"]):
        logger.critical("Output directory do not exist")
        quit(2)
    if cfg["runners_qty"] < 1 or \
       cfg["low_runner"] > cfg["high_runner"] or \
       cfg["low_runner"] > cfg["runners_qty"] or \
       cfg["high_runner"] > cfg["runners_qty"]:
        logger.critical("Invalid runner ID configuration")
        quit(3)

    if cfg["mode"] in ["reduce", "reduce_collect"]:
        if not os.path.isdir(cfg["input_dir"]):
            logger.critical("Input directory do not exist")
            quit(1)
        logger.info("Reduction started")
        # Spawn runners with id=low_runner..high_runner in separated subprocesses
        runners = []
        for i in range(cfg["low_runner"] - 1, cfg["high_runner"]):
            r = Runner(cfg, i)
            r.start()
            runners.append(r)
        # Wait runners to finish their job
        for r in runners:
            r.join()
    if cfg["mode"] in ["reduce_collect", "collect"]:
        logger.info("Collecting started")
        result = None
        # Read partial results of all runners, aggregate it and write to Combined.csv
        # Please adjust here if your result data in not in host FS. E.g. you may want to do that for
        # Hadoop or SFTP.
        for i in range(cfg["runners_qty"]):
            part_name = cfg["output_dir"] + os.sep + ".Combined_" + str(i) + ".csv"
            if os.path.isfile(part_name):
                logger.debug("Collecting part " + part_name)
                result_part =  pd.read_csv(part_name)
                if result is None:
                    result = result_part
                else:
                    result = pd.concat([result, result_part], ignore_index=True)
                os.unlink(part_name)
                if len(result.index) > cfg["compact_at"]:
                    result = compact_data(result)
            else:
                logger.debug("Part " + part_name + "is missing")
        if result is None:
            logger.warning("Collected result is empty")
            if os.path.isfile(cfg["output_dir"] + os.sep + "Combined.csv"):
                os.unlink(cfg["output_dir"] + os.sep + "Combined.csv")
        else:
            logger.debug("Postprocessing and saving collected result")
            compact_data(result).sort_values(by="Source IP", key=lambda x: x.apply(sorting_key), ignore_index=True).\
                to_csv(cfg["output_dir"] + os.sep + ".Combined.csv", index=False, line_terminator="\n")
            os.replace(cfg["output_dir"] + os.sep + ".Combined.csv", cfg["output_dir"] + os.sep + "Combined.csv")
    logger.info("Coordinator complete")
