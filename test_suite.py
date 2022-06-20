import unittest
import os
import shutil
import sys
import subprocess
import filecmp

# Case list:
# 1. No valid data = no result
# 2. All files are valid, single dir
# 3. All files are valid, directory tree
# 4. Some files to be ignored
# 5. Broken csv file to be ignored
# 6. Csv file w/o required data column - to be ignored
# 7. No input directory
# 8. No output directory
# 9. Multiprocess config
# 10. New "aaa bbb.csv" file added and correctly processed
# 11. New "aaa bbb ccc.csv" file added and correctly processed


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


def mass_copy(file_list, dest_suffix):
    for file in file_list:
        shutil.copy("test_data" + os.sep + file, "test_case_data" + os.sep + dest_suffix + file)


def run_reducer():
    python = sys.executable
    return subprocess.call([python, "reducer.py"])


class ReducerTestCase01(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        set_env("test_data" + os.sep + "test_config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(False, os.path.isfile("test_case_data" + os.sep + "Combined.csv"))


class ReducerTestCase02(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv"], "")
        set_env("test_data" + os.sep + "test_config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                         "test_data" + os.sep + "test_result_1.csv", shallow=False))


class ReducerTestCase03(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        os.mkdir("test_case_data" + os.sep + "subdir")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv"], "")
        mass_copy(["Asia Prod 3.csv", "NA Prod.csv"], "subdir" + os.sep)
        set_env("test_data" + os.sep + "test_config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                         "test_data" + os.sep + "test_result_1.csv", shallow=False))


class ReducerTestCase04(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv", "sometext.txt",
                   "Combined.csv"], "")
        set_env("test_data" + os.sep + "test_config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                         "test_data" + os.sep + "test_result_1.csv", shallow=False))


class ReducerTestCase05(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv", "Asia Dev 1.csv"], "")
        set_env("test_data" + os.sep + "test_config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                         "test_data" + os.sep + "test_result_2.csv", shallow=False))


class ReducerTestCase06(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv", "Asia Dev 2.csv"], "")
        set_env("test_data" + os.sep + "test_config_1.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                         "test_data" + os.sep + "test_result_1.csv", shallow=False))


class ReducerTestCase07(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv"], "")
        set_env("test_data" + os.sep + "test_config_1.env")
        os.environ["REDUCER_INPUT_DIR"] = "test_case_data_missing"

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(1, ret_code)


class ReducerTestCase08(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv"], "")
        set_env("test_data" + os.sep + "test_config_1.env")
        os.environ["REDUCER_OUTPUT_DIR"] = "test_case_data_missing"

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(2, ret_code)


class ReducerTestCase09(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv"], "")
        set_env("test_data" + os.sep + "test_config_2.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                                           "test_data" + os.sep + "test_result_1.csv", shallow=False))


class ReducerTestCase10(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv", "EU Prod.csv"], "")
        set_env("test_data" + os.sep + "test_config_2.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                                           "test_data" + os.sep + "test_result_3.csv", shallow=False))


class ReducerTestCase11(unittest.TestCase):

    def setUp(self):
        os.mkdir("test_case_data")
        mass_copy(["Asia Prod 1.csv", "Asia Prod 2.csv", "Asia Prod 3.csv", "NA Prod.csv", "EU Prod 1.csv"], "")
        set_env("test_data" + os.sep + "test_config_2.env")

    def tearDown(self) -> None:
        shutil.rmtree("test_case_data")
        cleanup_env()

    def test_process(self):
        ret_code = run_reducer()
        self.assertEqual(0, ret_code)
        self.assertEqual(True, filecmp.cmp("test_case_data" + os.sep + "Combined.csv",
                                           "test_data" + os.sep + "test_result_3.csv", shallow=False))


def suite():
    s = unittest.TestSuite()
    s.addTest(ReducerTestCase01())
    s.addTest(ReducerTestCase02())
    s.addTest(ReducerTestCase03())
    s.addTest(ReducerTestCase04())
    s.addTest(ReducerTestCase05())
    s.addTest(ReducerTestCase06())
    s.addTest(ReducerTestCase07())
    s.addTest(ReducerTestCase08())
    s.addTest(ReducerTestCase09())
    s.addTest(ReducerTestCase10())
    s.addTest(ReducerTestCase11())
    return s


if __name__ == "__main__":
    unittest.TextTestRunner().run(suite())
