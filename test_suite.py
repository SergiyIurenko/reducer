import unittest
import os
import shutil
import sys
import subprocess

# Case list:
# 1. No valid data = no result
# 2. All files are valid, single dir
# 3. All files are valid, directory tree
# 4. Some files to be ignored
# 5. Broken csv file to be ignored
# 6. Csv file w/o required data column - to be ignored
# 7. No input directory
# 8. No output directory
# 9. Single process config
# 10. Multiprocess config
# 11. New "aaa bbb.csv" file added and correctly processed
# 12. New "aaa bbb ccc.csv" file added and correctly processed


def cleanup_env():
    items = ["REDUCER_INPUT_DIR",
             "REDUCER_OUTPUT_DIR",
             "REDUCER_RUNNERS_QTY",
             "REDUCER_LOW_RUNNER",
             "REDUCER_HIGH_RUNNER",
             "REDUCER_MODE",
             "REDUCER_BATCH",
             "REDUCER_COMPACT_AT",
             "LOG_LEVEL"]

    for item in items:
        if item in os.environ:
            del os.environ[item]


def set_env(env_file):
    with open(env_file, mode="r") as f:
        env_def = f.read()
    for env_line in env_def.replace("\r", "").split("\n"):
        var_name, var_val = tuple(env_line.split("="))
        os.environ[var_name] = var_val


def run_reducer():
    python = sys.executable
    print(python)
    subprocess.call([python, "reducer.py"])


class ReducerTestCase01(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        shutil.copy("test_data" + os.sep + "Combined.csv", "test_case_data" + os.sep + "Combined.csv")
        shutil.copy("test_data" + os.sep + "sometext.txt", "test_case_data" + os.sep + "sometext.txt")
        set_env("test_data" + os.sep + "config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        run_reducer()
        self.assertEqual(False, os.path.isfile("test_case_data" + os.sep + "Combined.csv"))

class ReducerTestCase02(unittest.TestCase):
    def setUp(self):
        os.mkdir("test_case_data")
        shutil.copy("test_data" + os.sep + "Combined.csv", "test_case_data" + os.sep + "Combined.csv")
        shutil.copy("test_data" + os.sep + "sometext.txt", "test_case_data" + os.sep + "sometext.txt")
        set_env("test_data" + os.sep + "config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        run_reducer()
        self.assertEqual(False, os.path.isfile("test_case_data" + os.sep + "Combined.csv"))

def suite():
    suite = unittest.TestSuite()
    suite.addTest(ReducerTestCase01())
    return suite


if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())