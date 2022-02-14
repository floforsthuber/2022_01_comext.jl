# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# total number of products BE exports to WORLD, UK

function n_products(df::DataFrame, declarants::Vector{String}, partners::Vector{String}, flow::String)

    # subset dataframe
    df = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :FLOW => ByRow(x -> x == flow))
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out total
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # take out missing values
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> x != 0)) # take out 0 (there are some introduced by adding EU)
    df = df[:, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW, :PRODUCT_NC, :PERIOD, :VALUE_IN_EUROS]]

    # WORLD
    cols_grouping = ["DECLARANT_ISO", "FLOW", "PRODUCT_NC", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df_WORLD = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)
    row_WORLD = [df_WORLD.DECLARANT_ISO[1] "WORLD" flow df_WORLD.PERIOD[1] length(unique(df_WORLD.PRODUCT_NC)) sum(df_WORLD.VALUE_IN_EUROS)]

    # GB
    df_GB = subset(df, :PARTNER_ISO => ByRow(x -> x in partners))
    cols_grouping = ["DECLARANT_ISO", "FLOW", "PRODUCT_NC", "PERIOD"]
    gdf = groupby(df_GB, cols_grouping)
    df_GB = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)
    row_GB = [df_GB.DECLARANT_ISO[1] "GB" flow df_GB.PERIOD[1] length(unique(df_GB.PRODUCT_NC)) sum(df_GB.VALUE_IN_EUROS)]
    
    cols_names = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC_COUNT", "VALUE_IN_EUROS"]
    output = DataFrame(vcat(row_WORLD, row_GB), cols_names)

    return output
end


#a = n_products(df, ["BE"], ["GB"], "exports")

# timespan
years = string.(2001:2021)
months = lpad.(1:12, 2, '0')

df_fig5 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[], 
                    PRODUCT_NC_COUNT=Int64[], VALUE_IN_EUROS=Float64[])

for i in years
    for j in months

        #import and clean data
        df = initial_cleaning(i, j)

        # append data for figures/tables
        append!(df_fig5, n_products(df, ["BE"], ["GB"], "exports"))

        println(" ✓ Data for $j/$i has been successfully added. \n")
    end
end

CSV.write(dir_io * "clean/" * "df_fig5" * ".csv", df_fig5)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------


path = dir_io * "clean/" * "df_fig5" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# average product value
transform!(df, [:VALUE_IN_EUROS, :PRODUCT_NC_COUNT] => ByRow( (x, n) -> x/n) => :AVG_PROD_VALUE)

# HP filter
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
transform!(df, [:AVG_PROD_VALUE] => (x -> HP(x, 20)) => :AVG_PROD_VALUE_HP)


# product count
p = @df df plot(:DATE, :PRODUCT_NC_COUNT,
        group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="products", title="Belgian export product count: \n World vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_io * "clean/images/fig5/" * "fig5_" * "WORLD_GB" * "_product_count" * ".png") # export image
savefig(p, dir_dropbox * "results/images/fig5/" * "fig5_" * "WORLD_GB" * "_product_count" * ".png") # export image

# average product value
p = @df df plot(:DATE, :AVG_PROD_VALUE,
        group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros", title="Average Belgian export value per product: \n Total vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_io * "clean/images/fig5/" * "fig5_" * "WORLD_GB" * "_avg_product_value" * ".png") # export image
savefig(p, dir_dropbox * "results/images/fig5/" * "fig5_" * "WORLD_GB" * "_avg_product_value" * ".png") # export image

# HP
p = @df df plot(:DATE, :AVG_PROD_VALUE_HP,
        group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros", title="Average Belgian export value per product (HP): \n Total vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_io * "clean/images/fig5/" * "fig5_" * "WORLD_GB" * "_avg_product_value_HP" * ".png") # export image
savefig(p, dir_dropbox * "results/images/fig5/" * "fig5_" * "WORLD_GB" * "_avg_product_value_HP" * ".png") # export image
