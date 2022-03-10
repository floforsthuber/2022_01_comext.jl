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
CSV.write(dir_dropbox * "results/correspondence/" * "table_NACE_correspondence" * ".csv", df)
XLSX.writetable(dir_dropbox * "results/correspondence/" * "table_NACE_correspondence" * ".xlsx", df, overwrite=true)



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
