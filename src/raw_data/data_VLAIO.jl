# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script creating monthly data files
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, Dates

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# initia DataFrame
df_VLAIO = DataFrame(PERIOD=Int64[], PRODUCT_NC=String[], PRODUCT_NC_LAB1=String[], PRODUCT_NC_LAB2=String[], DECLARANT_ISO=String[],
                TRADE_TYPE=String[], PARTNER_ISO=String[], FLOW=String[], VALUE_IN_EUROS=Union{Missing, Float64}[], QUANTITY_IN_KG=Union{Missing, Float64}[])


# slightly modified functions
function append_WORLD(df::DataFrame)

    # sum over PARTNER
    cols_grouping = ["DECLARANT_ISO", "PRODUCT_NC", "PRODUCT_NC_LAB1", "PRODUCT_NC_LAB2", "TRADE_TYPE", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

    df_WORLD.TRADE_TYPE .= "total"
    df_WORLD.PARTNER_ISO .= "WORLD"

    df = vcat(df, df_WORLD)

    return df
end

function append_EU(df::DataFrame, EU::Vector{String})

    # subset and sum over EU ctrys
    df_EU = subset(df, :PARTNER_ISO => ByRow(x -> x in EU))
    cols_grouping = ["DECLARANT_ISO", "PRODUCT_NC", "PRODUCT_NC_LAB1", "PRODUCT_NC_LAB2", "TRADE_TYPE", "FLOW", "PERIOD"]
    gdf = groupby(df_EU, cols_grouping)
    df_EU = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

    df_EU.TRADE_TYPE .= "intra"
    df_EU.PARTNER_ISO .= "EU"

    df = vcat(df, df_EU)

    return df
end

# EU27 in Dutch
EU27 = ["Roemenië", "Griekenland", "Oostenrijk", "Polen", "Duitsland", "Spanje", "Hongarije", "Slovakije", "Italië", "Nederland",
       "Frankrijk", "Letland", "Kroatië", "Cyprus", "Malta", "Litouwen", "Slovenië", "Estland", "Portugal", "Finland", "Tsjechië", 
       "Luxemburg", "Zweden", "Denemarken", "Bulgarije", "Ierland", "België"]


# -------------------------------------------------------------------------------------------------------------------------------------------------------------


for i in string.(2014:2021)
    
    path = dir_dropbox * "rawdata/VLAIO/" * "dbo.FactProductFlows_PD8_" * i * ".csv"
    df = CSV.read(path, DataFrame, header=false, delim=",", quoted=true)

    # same formatting as comext
    cols_names = ["PERIOD", "PRODUCT_NC", "PRODUCT_NC_LAB1","PRODUCT_NC_LAB2", "DECLARANT_ISO", "TRADE_TYPE",
                    "PARTNER_ISO", "FLOW", "VALUE_IN_EUROS", "QUANTITY_IN_KG", "SUP_QUANTITY", "SUP_UNIT"]
    rename!(df, cols_names)
    df = df[:, Not([:SUP_QUANTITY, :SUP_UNIT])]
    transform!(df, [:PARTNER_ISO, :DECLARANT_ISO, :TRADE_TYPE] .=> ByRow(string), renamecols=false)

    # missing values for zeros
    # Notes:
    #   - a lot of observations have entries for VALUES but only zeros for QUANTITY
    #   - for example in 2014: 25_304 of 97_708 (~25%) observations lost
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(x == 0.0, missing, x)), renamecols=false)

    g(x) = x[1:6]
    df.PERIOD = g.(string.(df.PERIOD))
    df.PERIOD = parse.(Int64, df.PERIOD)

    df.PRODUCT_NC = lpad.(string.(df.PRODUCT_NC), 2, '0')

    df.FLOW = ifelse.(df.FLOW .== "X", "exports", "imports")

    # create WORLD/EU
    df = append_WORLD(df)
    df = append_EU(df, EU27)

    # append
    append!(df_VLAIO, df)

end

# export
CSV.write(dir_dropbox * "rawdata/" * "df_VLAIO" * ".csv", df_VLAIO)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
