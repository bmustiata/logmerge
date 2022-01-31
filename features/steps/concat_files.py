import datetime
import unittest

from behave import *

import tracemix

use_step_matcher("re")

test = unittest.TestCase()
test.maxDiff = None


class NewDateTime(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return cls(year=2022, month=1, day=29, hour=3, minute=1, tzinfo=None)

    @classmethod
    def now(cls):
        return cls(year=2022, month=1, day=29, hour=3, minute=1, tzinfo=None)

datetime.datetime = NewDateTime


@given("I have some files with different content")
def i_have_some_files_with_different_content(context):
    """
    :type context: behave.runner.Context
    """
    pass  # the actual files are in the test_data folder


@when("I run tracemix to mix the sources")
def i_run_tracemix_to_mix_the_sources(context):
    tracemix.main_no_click(
        window=False,
        window_start="",
        window_end="",
        output="/tmp/out.txt",
        files_to_mix=[
            "features/steps/test_data/file1.txt",
            "features/steps/test_data/file2.txt",
            "features/steps/test_data/multiline.txt",
        ])

    context.output_file = read_file("/tmp/out.txt")


@then("the output file contains all files concatenated")
def the_output_file_contains_all_files_concatenated(context):
    expected = read_file("features/steps/expected/several_files_concatenated.txt")
    test.assertEquals(
        expected, context.output_file,
        "The concatenated files had wrong content"
    )


@when("I run tracemix to mix the sources and filter between 23:40 until 23:50")
def i_run_tracemix_same_day(context):
    """
    :type context: behave.runner.Context
    """
    tracemix.main_no_click(
        window=False,
        window_start="23:40",
        window_end="23:50",
        output="/tmp/out.txt",
        files_to_mix=[
            "features/steps/test_data/file1.txt",
            "features/steps/test_data/file2.txt",
            "features/steps/test_data/multiline.txt",
        ])

    context.output_file = read_file("/tmp/out.txt")


@then("the output file contains only the lines between 23:40 until 23:50")
def check_tracemix_contains_only_sameday_lines(context):
    expected = read_file("features/steps/expected/several_files_same_day.txt")
    test.assertEquals(
        expected, context.output_file,
        "The concatenated files had wrong content"
    )


@when("I run tracemix to mix the sources and filter between 23:50 until 00:01")
def i_run_tracemix_day_passes_over(context):
    tracemix.main_no_click(
        window=False,
        window_start="23:50",
        window_end="00:01",
        output="/tmp/out.txt",
        files_to_mix=[
            "features/steps/test_data/file1.txt",
            "features/steps/test_data/file2.txt",
            "features/steps/test_data/multiline.txt",
        ])

    context.output_file = read_file("/tmp/out.txt")


@then("the output file contains only the lines from the previous day until today")
def check_tracemix_contains_only_filtered_days(context):
    expected = read_file("features/steps/expected/several_files_day_over_midnight.txt")
    test.assertEquals(
        expected, context.output_file,
        "The concatenated files had wrong content"
    )


@when("I run tracemix to mix the sources and filter using full dates between 23:50 until 00:01")
def i_runtracemix_with_absolute_dates(context):
    tracemix.main_no_click(
        window=False,
        window_start="2022.1.29 23:50",
        window_end="2022.1.30 00:01",
        output="/tmp/out.txt",
        files_to_mix=[
            "features/steps/test_data/file1.txt",
            "features/steps/test_data/file2.txt",
            "features/steps/test_data/multiline.txt",
        ])

    context.output_file = read_file("/tmp/out.txt")


def read_file(file_name: str) -> str:
    with open(file_name, "rt", encoding="utf-8") as f:
        return f.read()
