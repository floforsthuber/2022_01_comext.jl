# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script for CN classification update
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download CN8 code updates from RAMON
# for 2021: https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_CLS_DLD&StrNom=CN_2021&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC
# for 2022: https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_CLS_DLD&StrNom=CN_2022&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# manually

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import data into Julia and do initial cleaning
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "raw/correspondence/CN/" * "CN_2022_UPDATE_SINCE_1988" * ".xlsx"
df = DataFrame(XLSX.readtable(path, "relatnc1988_2022")...)
transform!(df, names(df) .=> ByRow(string), renamecols=false)
transform!(df, :Period => ByRow(x -> x[1:4]) => :PERIOD_START, :Period => ByRow(x -> x[end-3:end]) => :PERIOD_END) # seperate column
transform!(df, ["Origin code", "Destination code"] .=> ByRow(x -> replace(x, " " => "")) .=> [:ORIG_CODE, :DEST_CODE]) # remove whitespace
df = df[:, [:PERIOD_START, :PERIOD_END, :ORIG_CODE, :DEST_CODE]] # subset to clean columns

CSV.write(dir_io * "clean/" * "CN_update.csv", df)

