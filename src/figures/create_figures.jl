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

# Notes:
#   - figure 1: evolution of total imports/exports from BE to UK vis-a-vis FR, NL, DE and EU
#       + HP filter available, what do chose for λ?
#   - figure 2: evolution of imports/exports from BE to UK at 2digit level
#       + HP filter available, what do chose for λ?
#   - figure 3: evolution of share of imports/exports from BE to UK of total imports/exports from BE
#       + also add FR, DE, NL, EU27
#   - table 1: evolution of most important products at 2digit level
#       + shares per product available for bilteral trade (i.e. sum to 100%) or for total trade (i.e. sum to figure 3)
#       + could compute figure 3 from data of table 1 as well!
#
#   - all figures in values and unit values (unit prices):
#       + compute a price index as using a weighted sum
#   - include vertical lines for different Brexit events

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Brexit events
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# vertical line
# vline!([Date(2016,6,23)], label="", color=:black, lw=2) # referendum
# vline!([Date(2016,12,07)], label="") # vote to trigger Article 50
# vline!([Date(2017,03,29)], label="") # Article 50
# vline!([Date(2017,06,19)], label="") # negotiations commence
# vline!([Date(2019,03,21)], label="") # first extension of Article 50
# vline!([Date(2019,04,10)], label="") # second extension of Article 50
# vline!([Date(2019,10,28)], label="") # third extension of Article 50
# vline!([Date(2019,12,20)], label="") # withdrawl bill passes UK
# vline!([Date(2020,01,31)], label="") # exit of UK from EU
# vline!([Date(2020,12,20)], label="") # UK passes Trade and Cooperation Agreement
# vline!([Date(2020,12,31)], label="") # transition period ends
# vline!([Date(2021,04,27)], label="") # EU passes Trade and Cooperation Agreement

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# HP filter (Hodrick–Prescott)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

λ = 20

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "df_fig1" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
transform!(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> (x -> HP(x, λ)) .=> [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP])

# create unweighted unit prices
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
transform!(df, [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP] => ByRow((v,q) -> v/q) => :UNIT_PRICE_HP)

# manipulation
# Notes:
#   - there are exceptionally big observations in some months (needs further investigation), remove for now
transform!(df, :PRICE_INDEX => ByRow(x -> ifelse(x > 20_000, missing, x)), renamecols=false)
transform!(df, :PRICE_INDEX_HP => ByRow(x -> ifelse(x > 20_000 || x < 0, missing, x)), renamecols=false)


# create 3MMA as observations are very volatile
movingaverage(input::AbstractArray, n::Int64) = [i < n ? missing : mean(input[i-n+1:i]) for i in eachindex(input)]
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :PRICE_INDEX => (x -> movingaverage(x,3)) => :PRICE_INDEX_3MMA)


# plotting
for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "* flow*": values")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig1/" * "fig1_" * flow * "_values" * ".png") # export image locally
    savefig(p, dir_dropbox * "results/images/fig1/" * "fig1_" * flow * "_values" * ".png") # export image dropbox

    # price index (weighted unit prices)
    # Notes:
    #   - unit prices are extremely volatile, 3 MMA would be useful
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted)", title="Belgian "*flow*": price index (weighted)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig1/" * "fig1_" * flow * "_prices_weighted" * ".png")
    savefig(p, dir_dropbox * "results/images/fig1/" * "fig1_" * flow * "_prices_weighted" * ".png")

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX_3MMA,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted, 3MMA)", title="Belgian "*flow*": price index (weighted, 3MMA)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig1/" * "fig1_" * flow * "_prices_weighted_3MMA" * ".png")
    savefig(p, dir_dropbox * "results/images/fig1/" * "fig1_" * flow * "_prices_weighted_3MMA" * ".png") 

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :UNIT_PRICE,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (unweighted)", title="Belgian "*flow*": price index (unweighted)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig1/" * "fig1_" * flow * "_prices_unweighted" * ".png") 
    savefig(p, dir_dropbox * "results/images/fig1/" * "fig1_" * flow * "_prices_unweighted" * ".png") 

end

# plotting of HP
for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "*flow*": values (HP)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig1/HP/" * "fig1_" * flow * "_values" * "_HP" * ".png") # export image locally
    savefig(p, dir_dropbox * "results/images/fig1/HP/" * "fig1_" * flow * "_values" * "_HP" * ".png") # export image dropbox

    # price index (weighted unit prices)
    # Notes:
    #   - unit prices are extremely volatile, 3 MMA would be useful
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted)", title="Belgian "*flow*": price index (weighted, HP)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig1/HP/" * "fig1_" * flow * "_prices_weighted" * "_HP" * ".png")
    savefig(p, dir_dropbox * "results/images/fig1/HP/" * "fig1_" * flow * "_prices_weighted" * "_HP" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :UNIT_PRICE_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (unweighted)", title="Belgian "*flow*": price index (unweighted, HP)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig1/HP/" * "fig1_" * flow * "_prices_unweighted" * "_HP" * ".png") 
    savefig(p, dir_dropbox * "results/images/fig1/HP/" * "fig1_" * flow * "_prices_unweighted" * "_HP" * ".png") 

end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 4
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# requires PERIOD to be sorted when grouped
function rolling_std(input::AbstractArray, n::Int64)
    @assert 1 <= n <= length(input)
    output = missings(Float64, length(input)) # initialize vector (keep first n as missing)
    for i in eachindex(output)[n:end]
        output[i] = std(input[i-n+1:i])
    end
    return output
end

# rolling standard deviation
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX, :UNIT_PRICE, :VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP, :UNIT_PRICE_HP] 
                .=> (x -> rolling_std(x, 6)) .=> 
                [:STD_VALUE, :STD_QUANTITY, :STD_PRICE, :STD_UNIT, :STD_VALUE_HP, :STD_QUANTITY_HP, :STD_PRICE_HP, :STD_UNIT_HP])


for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :STD_UNIT_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "* flow*": std unit prices (unweighted, HP)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig4/HP/" * "fig4_" * flow * "_std" * "_prices_unweighted" * "_HP" * ".png") # export image locally
    savefig(p, dir_dropbox * "results/images/fig4/HP/" * "fig4_" * flow * "_std" * "_prices_unweighted" * "_HP" * ".png") # export image dropbox

end



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "df_fig2" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
transform!(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> (x -> HP(x, λ)) .=> [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP])

# create unweighted unit prices
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
transform!(df, [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP] => ByRow((v,q) -> v/q) => :UNIT_PRICE_HP)

# manipulation
# Notes:
#   - there are exceptionally big observations in some months (needs further investigation), remove for now
transform!(df, :PRICE_INDEX => ByRow(x -> ifelse(x > 20_000, missing, x)), renamecols=false)
transform!(df, :UNIT_PRICE => ByRow(x -> ifelse(x > 20_000, missing, x)), renamecols=false)

# plotting
# subsetting products for figures
products = 1:8:89

for flow in ["imports", "exports"]
    for i in products
    
    product_range = i:i+7
    end_range = string(product_range[end])
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS,
        group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "*flow*"values")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig2/" * "fig2_" * flow * "_" * end_range * "_values" * ".png") # export image
    savefig(p, dir_dropbox * "results/images/fig2/" * "fig2_" * flow * "_" * end_range * "_values" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX,
        group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Belgian "*flow*"unit prices (weighted)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig2/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * ".png")
    savefig(p, dir_dropbox * "results/images/fig2/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE,
        group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Belgian "*flow*"unit prices (unweighted)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig2/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * ".png") 
    savefig(p, dir_dropbox * "results/images/fig2/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * ".png") 

    end
end

for flow in ["imports", "exports"]
    for i in products
    
    product_range = i:i+7
    end_range = string(product_range[end])
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS_HP,
        group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "*flow*": values")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig2/HP/" * "fig2_" * flow * "_" * end_range * "_values" * "_HP" * ".png") # export image
    savefig(p, dir_dropbox * "results/images/fig2/HP/" * "fig2_" * flow * "_" * end_range * "_values" * "_HP" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX_HP,
        group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Belgian "*flow*": unit prices (weighted)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig2/HP/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * "_HP" * ".png")
    savefig(p, dir_dropbox * "results/images/fig2/HP/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * "_HP" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE_HP,
        group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Belgian "*flow*": unit prices (unweighted)")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig2/HP/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * "_HP" * ".png") 
    savefig(p, dir_dropbox * "results/images/fig2/HP/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * "_HP" * ".png") 

    end
end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 3
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "df_fig3" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
transform!(gdf, [:VALUE_SHARE, :QUANTITY_SHARE] .=> (x -> HP(x, λ)) .=> [:VALUE_SHARE_HP, :QUANTITY_SHARE_HP])


# plotting
for flow in ["imports", "exports"]

    # value share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :VALUE_SHARE,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Belgian "*flow*": value share")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig3/" * "fig3_" * flow * "_value_share" * ".png") # export image
    savefig(p, dir_dropbox * "results/images/fig3/" * "fig3_" * flow * "_value_share" * ".png") # export image

    # quantity share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :QUANTITY_SHARE,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="kg", title="Belgian "*flow*": quantity share")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig3/" * "fig3_" * flow * "_quantity_share" * ".png") # export image
    savefig(p, dir_dropbox * "results/images/fig3/" * "fig3_" * flow * "_quantity_share" * ".png") # export image

end

# plotting HP
for flow in ["imports", "exports"]

    # value share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :VALUE_SHARE_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Belgian "*flow*": value share")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig3/HP/" * "fig3_" * flow * "_value_share" * "_HP" * ".png") # export image
    savefig(p, dir_dropbox * "results/images/fig3/HP/" * "fig3_" * flow * "_value_share" * "_HP" * ".png") # export image

    # quantity share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :QUANTITY_SHARE_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="kg", title="Belgian "*flow*": quantity share")
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig3/HP/" * "fig3_" * flow * "_quantity_share" * "_HP" * ".png") # export image
    savefig(p, dir_dropbox * "results/images/fig3/HP/" * "fig3_" * flow * "_quantity_share" * "_HP" * ".png") # export image

end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Table 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "df_tab1" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :YEAR => ByRow(x -> Date(string(x), DateFormat("yyyy"))) => :DATE)

df_tab1 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], YEAR=Int64[],
                    PRODUCT_NC_digits=Int64[], VALUE_SHARE_TOTAL=Float64[])


tab = subset(df, :PARTNER_ISO => ByRow(x -> x == "GB"), :FLOW => ByRow(x -> x == "imports"), :YEAR => ByRow(x -> x == 2001))
tab = tab[:, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW, :YEAR, :PRODUCT_NC_digits, :VALUE_SHARE_PARTNER]]
sort!(tab, order(:VALUE_SHARE_TOTAL, rev=true))


for i in 2001:2021
    
    tab = subset(df, :PARTNER_ISO => ByRow(x -> x == "GB"), :FLOW => ByRow(x -> x == "imports"), :YEAR => ByRow(x -> x == i))
    tab = tab[:, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW, :YEAR, :PRODUCT_NC_digits, :VALUE_SHARE_TOTAL]]
    sort!(tab, order(:VALUE_SHARE_TOTAL, rev=true))
    append!(df_tab1, first(tab, 10))

end



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 4
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# uses same data as fig1
path = dir_io * "clean/" * "df_fig1" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# create unweighted unit prices
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

# manipulation
# Notes:
#   - there are exceptionally big observations in some months (needs further investigation), remove for now
transform!(df, :PRICE_INDEX => ByRow(x -> ifelse(x > 20_000, missing, x)), renamecols=false)


# requires PERIOD to be sorted when grouped
function rolling_std(input::AbstractArray, n::Int64)
    @assert 1 <= n <= length(input)
    output = missings(Float64, length(input)) # initialize vector (keep first n as missing)
    for i in eachindex(output)[n:end]
        output[i] = std(input[i-n+1:i])
    end
    return output
end

# rolling standard deviation
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX, :UNIT_PRICE] .=> (x -> rolling_std(x, 6)) .=> [:STD_VALUE, :STD_QUANTITY, :STD_PRICE, :STD_UNIT])

# for flow in ["imports", "exports"]

#     # values
#     # Notes:
#     #   - take out EU for scale
#     p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :STD_VALUE,
#             group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros?", title="Belgian "* flow*": std values")
#     vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
#     vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
#     vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
#     savefig(p, dir_io * "clean/images/fig4/" * "fig4_" * flow * "_std" * "_values" * ".png") # export image locally
#     savefig(p, dir_dropbox * "results/images/fig4/" * "fig4_" * flow * "_std" * "_values" * ".png") # export image dropbox

# end


p = @df subset(df, :FLOW => ByRow(x -> x == "imports")) plot(:DATE, :STD_UNIT,
group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros?", title="Belgian "* "exports" *": std values")
vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
