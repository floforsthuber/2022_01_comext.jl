# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script with functions to import and transform raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download CN8 code updates from RAMON
# https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_CLS_DLD&StrNom=CN_2021&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

url = "https://ec.europa.eu/eurostat/ramon/documents/cn_2021/CN_2021_UPDATE_SINCE_1988.zip"
path = dir_io * "raw/correspondence/" * "CN_2021_UPDATE_SINCE_1988.zip"

download(url, path)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import data into Julia and do initial cleaning
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = DataFrame(XLSX.readtable(dir_io * "raw/correspondence/" * "CN_2021_UPDATE_SINCE_1988" * ".xlsx", "relatnc1988_2021")...)
transform!(df, names(df) .=> ByRow(string), renamecols=false)
transform!(df, :Period => ByRow(x -> x[1:4]) => :PERIOD_START, :Period => ByRow(x -> x[end-3:end]) => :PERIOD_END) # seperate column
transform!(df, ["Origin code", "Destination code"] .=> ByRow(x -> replace(x, " " => "")) .=> [:ORIG_CODE, :DEST_CODE]) # remove whitespace
df = df[:, [:PERIOD_START, :PERIOD_END, :ORIG_CODE, :DEST_CODE]] # subset to clean columns


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Gather product codes for which have been changes in sample (2015-2021)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Notes:
#   - need product codes which have been introduced or discontinued during timespan 2015-2021
years = string.(2015:2021)
subset!(df, :PERIOD_START => ByRow(x -> x in years)) # subset for our timespan