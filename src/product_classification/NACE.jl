# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script for NACE classification 
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download NACE Rev.2 structure from RAMON
# https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_CLS_DLD&StrNom=NACE_REV2&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC#
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# manually

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import data into Julia and do cleaning
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df_NACE = CSV.read(dir_dropbox * "rawdata/correspondence/" * "NACE_rev2" * ".csv", DataFrame)

# only keep columns for correspondence
cols_keep = ["Level", "Code", "Parent", "Description", "Reference to ISIC Rev. 4"]
df = df_NACE[:, cols_keep]

# rename columns
cols_name = ["NACE_level", "NACE_code", "NACE_parent", "NACE_lab", "ISIC4_code"]
rename!(df, names(df) .=> cols_name)

# formatting
transform!(df, names(df) .=> ByRow(string), renamecols=false) # missing become strings
transform!(df, [:NACE_code, :NACE_parent] .=> ByRow(x -> replace(x, "." => "")), renamecols=false) # remove dots
transform!(df, names(df) .=> ByRow(x -> ifelse(x == "missing", missing, x)), renamecols=false) # introduce missing again

# export
# CSV.write(dir_dropbox * "results/correspondence/" * "table_NACE_correspondence" * ".csv", df)
# XLSX.writetable(dir_dropbox * "results/correspondence/" * "table_NACE_correspondence" * ".xlsx", df, overwrite=true)

# add some custom labels

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# CN and PRODCOM correspondence

df_CN_PROD = DataFrame(XLSX.readtable(dir_dropbox* "rawdata/correspondence/" * "CN_2020_PRODCOM_2019" * ".xlsx", "CN2020_PRODCOM2019")...)
rename!(df_CN_PROD, names(df_CN_PROD) .=> [:CN_code, :PRODCOM_code])
transform!(df_CN_PROD, names(df_CN_PROD) .=> ByRow(x -> lpad(replace(x, " " => ""), 8, '0')), renamecols=false)
transform!(df_CN_PROD, :PRODCOM_code => ByRow(x -> x[1:2]) => :NACE_code)

df_NACE = DataFrame(XLSX.readtable(dir_dropbox* "results/correspondence/" * "table_NACE_custom" * ".xlsx", "Sheet1")...)
transform!(df_NACE, names(df_NACE) .=> ByRow(string), renamecols=false)

# join
df_join = leftjoin(df_CN_PROD, df_NACE, on=:NACE_code)
transform!(df_join, names(df_join) .=> ByRow(string), renamecols=false)

# XLSX.writetable(dir_dropbox * "results/correspondence/" * "table_CN_PRODCOM" * ".xlsx", df_join, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# CN and CPA correspondence
df_CN_CPA = CSV.read(dir_dropbox * "rawdata/correspondence/" * "CN2020_CPA21" * ".csv", DataFrame)
df_CN_CPA = df_CN_CPA[2:end,1:2]
rename!(df_CN_CPA, names(df_CN_CPA) .=> [:PRODUCT_NC, :CPA21_code])
transform!(df_CN_CPA, :PRODUCT_NC => ByRow(x -> lpad(replace(x, " " => ""), 8, '0')), renamecols=false)
transform!(df_CN_CPA, :CPA21_code => ByRow(x -> replace(x, "." => "")), renamecols=false)
transform!(df_CN_CPA, :CPA21_code => ByRow(x -> x[1:2]) => :CPA21_2digits)

df_CPA = CSV.read(dir_dropbox * "rawdata/correspondence/" * "CPA21" * ".csv", DataFrame)
df_CPA = df_CPA[:, ["Level", "Code", "Parent", "Description"]]
rename!(df_CPA, names(df_CPA) .=> ["level", "CPA21_code", "parent", "CPA21_lab"])
transform!(df_CPA, names(df_CPA) .=> ByRow(string), renamecols=false)
transform!(df_CPA, [:CPA21_code, :parent] .=> ByRow(x -> replace(x, "." => "")), renamecols=false)

subset!(df_CPA, :CPA21_code => ByRow(x -> length(x) == 2))
rename!(df_CPA, :CPA21_code => :CPA21_2digits)

# XLSX.writetable(dir_dropbox * "results/correspondence/" * "table_CPA21" * ".xlsx", df_CPA[:,["CPA21_2digits", "CPA21_lab"]], overwrite=true)

df_CPA = DataFrame(XLSX.readtable(dir_dropbox* "results/correspondence/" * "table_CPA21" * ".xlsx", "Sheet1")...)


df_join = leftjoin(df_CN_CPA, df_CPA, on=:CPA21_2digits)

XLSX.writetable(dir_dropbox * "results/correspondence/" * "table_CN_CPA21" * ".xlsx", df_join, overwrite=true)
