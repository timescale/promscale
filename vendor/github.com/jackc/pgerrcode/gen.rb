# Run this script against the data in table A.1. on https://www.postgresql.org/docs/11/errcodes-appendix.html.
#
# Source data should be formatted like the following:
#
# Class 00 — Successful Completion
# 00000 	successful_completion
# Class 01 — Warning
# 01000 	warning
# 0100C 	dynamic_result_sets_returned

code_name_overrides = {
  # Some error code names are repeated. In those cases add the error class as a suffix.
  "01004" => "StringDataRightTruncationWarning",
  "22001" => "StringDataRightTruncationDataException",
  "22004" => "NullValueNotAllowedDataException",
  "2F002" => "ModifyingSQLDataNotPermittedSQLRoutineException",
  "2F003" => "ProhibitedSQLStatementAttemptedSQLRoutineException",
  "2F004" => "ReadingSQLDataNotPermittedSQLRoutineException",
	"38002" => "ModifyingSQLDataNotPermittedExternalRoutineException",
	"38003" => "ProhibitedSQLStatementAttemptedExternalRoutineException",
  "38004" => "ReadingSQLDataNotPermittedExternalRoutineException",
  "39004" => "NullValueNotAllowedExternalRoutineInvocationException",

  # Go casing corrections
	"08001" => "SQLClientUnableToEstablishSQLConnection",
  "08004" => "SQLServerRejectedEstablishmentOfSQLConnection",
  "P0000" => "PLpgSQLError"
}

ARGF.each do |line|
  case line
  when /^Class/
    puts
    puts "// #{line}"
  when /^(\w{5})\s+(\w+)/
    code = $1
    name = code_name_overrides.fetch(code) do
      $2.split("_").map(&:capitalize).join
        .gsub("Sql", "SQL")
        .gsub("Xml", "XML")
        .gsub("Fdw", "FDW")
        .gsub("Srf", "SRF")
        .gsub("Io", "IO")
    end
    puts %Q[#{name} = "#{code}"]
  else
    puts line
  end
end
