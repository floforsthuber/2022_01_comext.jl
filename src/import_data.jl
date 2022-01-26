# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script with functions to import and transform raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


using DataFrames, CSV, XLSX, LinearAlgebra, Statistics


dir_raw = "C:/Users/u0148308/data/raw/" # location of raw data

url = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F"

years = string.(2015:2021)
months = lpad.(1:12, 2, '0')

for i in years
    for j in months
        
        id = "full" * i * j * ".7z"

        if !isfile(dir_raw * "comext/zipped/" * id)
            download(url * id, dir_raw * "comext/zipped/" * id)
        else
            println("The zipped file for $j/$i has already been downloaded.")
        end

    end
end

# -----------
# NEED TO MANUALLY UNZIPP to folder dir_raw!
# trying to automate un-zipping, unfortunately failed
# run(`cmd /c set PATH=%PATH% ';' "C:\\Program Files\\7-Zip\\" echo %PATH% 7z`)
# run(`cmd /c cd "C:\\Users\\u0148308\\data\\raw\\"`)
# run(`cmd /c 7z e a.7z`)

# -----------

# initialize DataFrame
# Notes:
#   - column names and types need to correspond to the final output of the "inital_cleaning" function
df = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], TRADE_TYPE=String[], PRODUCT_NC=String[], FLOW=String[], PERIOD=Int64[],
                VALUE_IN_EUROS=Union{Missing, Float64}[], QUANTITY_IN_KG=Union{Missing, Float64}[])

# function does the initial cleaning of the data               
function initial_cleaning(path::String)
    
    df = CSV.read(path, DataFrame)

    # correct column types
    # Notes:
    #   - how to be treat PERIOD best, string or numeric? leave as Int64 for now
    transform!(df, [:DECLARANT, :PARTNER] .=> ByRow(Int64), renamecols=false)
    transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :TRADE_TYPE, :PRODUCT_NC, :PRODUCT_SITC, :PRODUCT_CPA2002, :PRODUCT_CPA2008, :PRODUCT_CPA2_1,
                    :PRODUCT_BEC, :PRODUCT_BEC5, :PRODUCT_SECTION, :FLOW, :STAT_REGIME] .=> ByRow(string), renamecols=false)
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :SUP_QUANTITY] .=> ByRow(Float64), renamecols=false)

    # rename some indicators
    df[:, :TRADE_TYPE] .= ifelse.(df[:, :TRADE_TYPE] .== "I", "intra", "extra")
    df[:, :FLOW] .= ifelse.(df[:, :FLOW] .== "1", "imports", "exports")
    df[:, :SUPP_UNIT] .= ifelse.(ismissing.(df[:, :SUPP_UNIT]), missing, string.(df[:, :SUPP_UNIT])) # type still weird, should be Union{Missing, String}

    # missing data
    # Notes:
    #   - if value or quanity is reported as 0 use missing instead
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :SUP_QUANTITY] .=> ByRow(x->ifelse(x == 0.0, missing, x)), renamecols=false)

    # aggregate over STAT_REGIME, SUPP_UNIT
    # Notes:
    #   - only lose few rows ~1%
    #   - disregard SUPP_UNIT and SUP_QUANTITY for the time being
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

    return df
end

# # -----------

# years = string.(2019:2020)
# months = lpad.(1:12, 2, '0')

# for i in years
#     for j in months
#         path = dir_raw * "comext/" * "full" * i * j * ".dat"
#         append!(df, initial_cleaning(path))
#     end
# end

# # compute UNIT_PRICE
# transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

# # custom function since no predefined function
# function pct_change(input::AbstractVector{<:Number})
#     [i == 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
# end

# # -----------


path = dir_raw * "comext/" * "full" * "2020" * "01" * ".dat"
append!(df, initial_cleaning(path))
path = dir_raw * "comext/" * "full" * "2020" * "02" * ".dat"
append!(df, initial_cleaning(path))

sort!(df)

# compute UNIT_PRICE
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

# custom function since no predefined function
function pct_change(input::AbstractVector)
    [i == 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
end



cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "TRADE_TYPE", "PRODUCT_NC", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :UNIT_PRICE => pct_change => :UNIT_PRICE_CHANGE)
