# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Notes:
#   - import raw data, initial cleaning
#   - subset and compute data for figure
#   - loop over months and add to DataFrame
#   - export data
#   - import data and plot figures (different script)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# timespan
years = string.(2001:2021)
months = lpad.(1:12, 2, '0')

# EU27
ctrys_EU27 = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU",
             "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

# initialize dataframes
df_fig1 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[],
                    VALUE_IN_EUROS=Float64[], QUANTITY_IN_KG=Float64[], PRICE_INDEX=Float64[])

df_fig2 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[], PRODUCT_NC_digits=String[],
                    VALUE_IN_EUROS=Float64[], QUANTITY_IN_KG=Float64[], PRICE_INDEX=Float64[])

df_fig3 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[], 
                    VALUE_SHARE=Float64[], QUANTITY_SHARE=Float64[])

df_tab1 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], YEAR=String[], PRODUCT_NC_digits=String[],
                    VALUE_SHARE_PARTNER=Float64[], QUANTITY_SHARE_PARTNER=Float64[], VALUE_SHARE_TOTAL=Float64[], QUANTITY_SHARE_TOTAL=Float64[])

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

for i in years
    for j in months

        #import and clean data
        df = initial_cleaning(i, j)

        # create EU as partner
        df = append_EU(df, ctrys_EU27)

        # append data for figures/tables
        append!(df_fig1, data_fig1(df, ["BE"], ["GB", "EU", "DE", "FR", "NL"]))
        append!(df_fig2, data_fig2(df, ["BE"], ["GB"], 2))
        append!(df_fig3, data_fig3(df, ["BE"], ["GB", "EU", "DE", "FR", "NL"]))
        append!(df_tab1, tab1(df, ["BE"], ["GB", "EU", "DE", "FR", "NL"], 2))


        println(" ✓ Data for $j/$i has been successfully added. \n")
    end
end

# export 
CSV.write(dir_io * "clean/" * "df_fig1" * ".csv", df_fig1)
CSV.write(dir_io * "clean/" * "df_fig2" * ".csv", df_fig2)
CSV.write(dir_io * "clean/" * "df_fig3" * ".csv", df_fig3)
CSV.write(dir_io * "clean/" * "df_tab1" * ".csv", df_tab1)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------

