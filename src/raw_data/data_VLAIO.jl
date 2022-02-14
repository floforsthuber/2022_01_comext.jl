# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script creating monthly data files
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

year = string(2021)

path = dir_dropbox * "rawdata/VLAIO/" * "dbo.FactProductFlows_PD8_" * year * ".csv"
df = CSV.read(path, DataFrame, header=false, delim=",", quoted=true)

# same formatting as comext
cols_names = ["PERIOD", "PRODUCT_NC", "PRODUCT_NC_LAB1","PRODUCT_NC_LAB2", "DECLARANT", "TRADE_TYPE",
                 "PARTNER", "FLOW", "VALUE_IN_EUROS", "QUANTITY_IN_KG", "SUP_QUANTITY", "SUPP_UNIT"]
rename!(df, cols_names)

g(x) = x[1:6]
df.PERIOD = g.(string.(df.PERIOD))
df.PERIOD = parse.(Int64, df.PERIOD)

df.PRODUCT_NC = lpad.(string.(df.PRODUCT_NC), 2, '0')

df.FLOW = ifelse.(df.FLOW .== "X", "exports", "imports")