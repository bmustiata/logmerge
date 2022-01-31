from behave import *

use_step_matcher("re")


@given("I have two files with different content")
def i_have_two_files_with_different_content(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Given I have two files with different content')


@when("I run tracemix to mix the sources")
def i_run_tracemix_to_mix_the_sources(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: When I run tracemix to mix the sources')


@then("the output file contains both files concatenated")
def the_output_file_contains_both_files_concatenated(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then the output file contains both files concatenated')


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