Feature: tracemix should be able to concat files

@1
Scenario: several files should be concatenated with the lines interleaved
  Given I have some files with different content
  When I run tracemix to mix the sources
  Then the output file contains all files concatenated

@2
Scenario: filtering by time should drop lines outside of the time range in the same day
  Given I have some files with different content
  When I run tracemix to mix the sources and filter between 23:40 until 23:50
  Then the output file contains only the lines between 23:40 until 23:50

@3
Scenario: filtering by time should drop lines outside of the time range with date pass
  Given I have some files with different content
  When I run tracemix to mix the sources and filter between 23:50 until 00:10
  Then the output file contains only the lines from the previous day until today

@4
Scenario: filtering by date-time should drop lines outside the absolute time range
  Given I have some files with different content
  When I run tracemix to mix the sources and filter using full dates between 23:50 until 00:10
  Then the output file contains only the lines from the previous day until today

