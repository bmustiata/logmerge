import subprocess
import unittest

from behave import *

use_step_matcher("re")

test = unittest.TestCase()
test.maxDiff = None


@given("I have some files with different content")
def i_have_some_files_with_different_content(context):
    """
    :type context: behave.runner.Context
    """
    pass  # the actual files are in the test_data folder


@when("I run tracemix to mix the sources")
def i_run_tracemix_to_mix_the_sources(context):
    subprocess.check_call([
        "python", "tracemix.py", "--out", "/tmp/out.txt", 
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
    raise NotImplementedError(u'STEP: When I run tracemix to mix the sources and filter between 23:40 until 23:50')


@then("the output file contains only the lines between 23:40 until 23:50")
def check_tracemix_contains_only_sameday_lines(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then the output file contains only the lines between 23:40 until 23:50')


@when("I run tracemix to mix the sources and filter between 23:50 until 00:10")
def i_run_tracemix_day_passes_over(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: When I run tracemix to mix the sources and filter between 23:50 until 00:10')


@then("the output file contains only the lines from the previous day until today")
def check_tracemix_contains_only_filtered_days(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then the output file contains only the lines from the previous day until today')


@when("I run tracemix to mix the sources and filter using full dates between 23:50 until 00:10")
def i_runtracemix_with_absolute_dates(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(
        u'STEP: When I run tracemix to mix the sources and filter using full dates between 23:50 until 00:10')


def read_file(file_name: str) -> str:
    with open(file_name, "rt", encoding="utf-8") as f:
        return f.read()
