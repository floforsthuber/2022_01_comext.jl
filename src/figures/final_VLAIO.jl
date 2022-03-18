# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics (VALUES)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates, Pipe

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df_VLAIO = CSV.read(dir_dropbox * "rawdata/" * "df_VLAIO" * ".csv", DataFrame)
transform!(df_VLAIO, ["PRODUCT_NC", "PRODUCT_NC_LAB1", "PRODUCT_NC_LAB2", "DECLARANT_ISO", "TRADE_TYPE", "PARTNER_ISO", "FLOW"] .=> ByRow(string), renamecols=false)
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> convert(Union{Missing, Float64}, x)), renamecols=false)
df_VLAIO.PRODUCT_NC = lpad.(string.(df_VLAIO.PRODUCT_NC), 8, '0') # needs to be done again

# remove missing, only for VALUES
#   - drop missing [VALUE, QUANTITY]: lose 1-16027346/21667553 ~26% observations
#   - drop missing VALUE: lose 1-21666543/21667553 ~ 0.1% observations
subset!(df_VLAIO, :VALUE_IN_EUROS .=> ByRow(x -> !ismissing(x)))

# not needed in case of VALUES!!
# MAD adjustment
#   - cutoff = 3: lose further 1-15177297/21666543 ~30% observations (~30% of total)
#   - cutoff = 4: lose further 1-15812254/21666543 ~27% observations (~27% of total)
# outlier_cutoff = 4
# gdf = groupby(df_VLAIO, "PRODUCT_NC")
# df_VLAIO = transform(gdf, :VALUE_IN_EUROS => MAD_method => :MAD)
# subset!(df_VLAIO, :MAD => ByRow(x -> x < outlier_cutoff))
# df_VLAIO = df_VLAIO[:, Not(:MAD)]


# add EU/WORLD again
EU27 = ["Roemenië", "Griekenland", "Oostenrijk", "Polen", "Duitsland", "Spanje", "Hongarije", "Slovakije", "Italië", "Nederland",
       "Frankrijk", "Letland", "Kroatië", "Cyprus", "Malta", "Litouwen", "Slovenië", "Estland", "Portugal", "Finland", "Tsjechië", 
       "Luxemburg", "Zweden", "Denemarken", "Bulgarije", "Ierland", "België"]

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

df_VLAIO = append_WORLD(df_VLAIO)
df_VLAIO = append_EU(df_VLAIO, EU27)

# double check if no zeros/missing introduced by WORLD/EU
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false)
subset!(df_VLAIO, :VALUE_IN_EUROS => ByRow(x -> !ismissing(x))) # lose 0% observations

cols_subset = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC", "VALUE_IN_EUROS", "QUANTITY_IN_KG"]
df_VLAIO = df_VLAIO[:, cols_subset]


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 1
#   - Export/import value
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# prepare data
df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "Duitsland", "Nederland", "Frankrijk", "Italië"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

# formatting
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# need to sort before computing HP and 3MMA
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :VALUE_IN_EUROS => (x -> HP(x, λ)) => :VALUE_IN_EUROS_HP)


# plotting
#   TO DO:
#       - translate title!
line_color = [palette(:tab10)[1:3]; palette(:tab10)[7]; palette(:tab10)[4]]';
tick_years = Date.(2014:2021)
DateTick = Dates.format.(tick_years, "yyyy")

for flow in ["imports", "exports"]

    # HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :VALUE_IN_EUROS_HP/1e9, linecolor=line_color, xticks=false,
            group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euro (miljard)", title="Flemish "*flow*": values (HP, λ=$λ)")
    plot!(xticks=(tick_years, DateTick))
    vline!([Date(2016,6,23)], label="", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/formatted/" * "fig1_" * flow * "_values" * "_HP" * ".png")

 
end

df_export = subset(df, :FLOW => ByRow(x -> x == "exports"))[:,Not([:VALUE_IN_EUROS])]
XLSX.writetable(dir_dropbox * "results/images/VLAIO/formatted/" * "data_figure1" * ".xlsx", df_export, overwrite=true)

df_export = subset(df, :FLOW => ByRow(x -> x == "imports"))[:,Not([:VALUE_IN_EUROS])]
XLSX.writetable(dir_dropbox * "results/images/VLAIO/formatted/" * "data_figure2" * ".xlsx", df_export, overwrite=true)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 3
#   - Export/import share
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# prepare data
df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "WORLD"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

df_WORLD = subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD"))
cols_grouping = ["DECLARANT_ISO", "FLOW", "PERIOD"]
gdf = groupby(df_WORLD, cols_grouping)
df_WORLD = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_WORLD)

df = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), df_WORLD, on=[:DECLARANT_ISO, :FLOW, :PERIOD])
transform!(df, [:VALUE_IN_EUROS, :VALUE_WORLD] => ByRow((x,s) -> x/s) => :SHARE_VALUE)

# Date
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :SHARE_VALUE => (x -> HP(x, λ)) => :SHARE_VALUE_HP)

# plotting
#   TO DO:
#       - translate title!
line_color = [:red; :green]';
tick_years = Date.(2014:2021)
DateTick = Dates.format.(tick_years, "yyyy")

# VLA vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :SHARE_VALUE_HP*100, linecolor=[palette(:tab10)[3] palette(:tab10)[4]], xticks=false,
        group=:FLOW, lw=2, legend=:topright, ylabel="percentages", title="Trade share (values, λ=$λ): \n Flanders vis-a-vis Great Britain")
    plot!(xticks=(tick_years, DateTick))
    vline!([Date(2016,6,23)], label="", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/VLAIO/formatted/" * "fig3_" * "VLA_GB" * "_value_share" * "_HP" * ".png") # export image

df_export = subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"))[:,["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "DATE", "SHARE_VALUE_HP"]]
XLSX.writetable(dir_dropbox * "results/images/VLAIO/formatted/" * "data_figure3" * ".xlsx", df_export, overwrite=true)



# --------------
# Figure 8
#   - Export/import share of top 10 industries (2 digit NACE)
# --------------

df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))
transform!(df, :PERIOD => ByRow(x -> Date(string(x)[1:4], DateFormat("yyyy"))) => :YEAR)

# CPA classification
df_CN_PROD = DataFrame(XLSX.readtable(dir_dropbox* "results/correspondence/" * "table_CN_CPA21" * ".xlsx", "Sheet1")...)
transform!(df_CN_PROD, names(df_CN_PROD) .=> ByRow(string), renamecols=false)

df_join = leftjoin(df, df_CN_PROD, on=:PRODUCT_NC)
subset!(df_join, :CPA21_lab => ByRow(x -> !ismissing(x))) # lose 1-7019070/7260237 ~ 3%

# aggregate over products and country
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR", "CPA21_2digits" , "CPA21_lab", "CPA21_lab_custom"]
gdf = groupby(df_join, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

# aggregate over products
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "YEAR", "FLOW"]
gdf = groupby(df_join, cols_grouping)
df_total_partner = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL)
transform!(df_total_partner, :YEAR => ByRow(x -> string(x)[1:4]), renamecols=false)

# subset products top 10 and produce share
transform!(df, :YEAR => ByRow(x -> string(x)[1:4]), renamecols=false)
top10_exports = first(sort(subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"), :YEAR => ByRow(x -> x == "2021"),
     :FLOW => ByRow(x -> x == "exports")), order(:VALUE_IN_EUROS, rev=true)).CPA21_2digits, 10)
top10_imports = first(sort(subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"), :YEAR => ByRow(x -> x == "2021"),
     :FLOW => ByRow(x -> x == "imports")), order(:VALUE_IN_EUROS, rev=true)).CPA21_2digits, 10)

# join
df = leftjoin(df, df_total_partner, on=[:DECLARANT_ISO, :PARTNER_ISO, :FLOW, :YEAR])
transform!(df, [:VALUE_IN_EUROS, :VALUE_TOTAL] => ByRow((x, y) -> x/y) => :SHARE_VALUE)
transform!(df, [:CPA21_2digits, :CPA21_lab_custom] => ByRow((x,y) -> x * " " * y) => :legend)

sort!(df)

# plotting
#   TO DO:
#       - translate title!
tick_years = Date.(2014:2021)
DateTick = Dates.format.(tick_years, "yyyy")
line_color = palette(:tab10)[1:10]';

flow = "exports"
p = @df subset(df, :FLOW => ByRow(x -> x == flow), :CPA21_2digits => ByRow(x -> x in top10_exports)) groupedbar(:YEAR, :SHARE_VALUE*100, color=line_color,
    group=:legend, bar_position=:stack, legend=:outertopleft, ylabel="percentages", title="Flemish "* flow*" with GB: \n Top 10 industries (2021)", xrotation=35)
savefig(p, dir_dropbox * "results/images/VLAIO/formatted/" * "fig8_" * flow * "_values_annual" * "_top10_2021" * ".png") # export image dropbox

df_export = subset(df, :FLOW => ByRow(x -> x == flow), :CPA21_2digits => ByRow(x -> x in top10_exports))[:,["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR", "legend", "SHARE_VALUE"]]
XLSX.writetable(dir_dropbox * "results/images/VLAIO/formatted/" * "data_figure6" * ".xlsx", df_export, overwrite=true)


flow = "imports"
p = @df subset(df, :FLOW => ByRow(x -> x == flow), :CPA21_2digits => ByRow(x -> x in top10_imports)) groupedbar(:YEAR, :SHARE_VALUE*100, color=line_color,
    group=:legend, bar_position=:stack, legend=:outertopleft, ylabel="percentages", title="Flemish "* flow*" with GB: \n Top 10 industries (2021)", xrotation=35)
savefig(p, dir_dropbox * "results/images/VLAIO/formatted/" * "fig8_" * flow * "_values_annual" * "_top10_2021" * ".png") # export image dropbox

df_export = subset(df, :FLOW => ByRow(x -> x == flow), :CPA21_2digits => ByRow(x -> x in top10_imports))[:,["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR", "legend", "SHARE_VALUE"]]
XLSX.writetable(dir_dropbox * "results/images/VLAIO/formatted/" * "data_figure7" * ".xlsx", df_export, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 5
#   - Export/import number of products
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# prepare data
df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "Duitsland", "Nederland", "Frankrijk", "Italië"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, nrow => :COUNT)

transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :COUNT => (x -> HP(x, λ)) => :COUNT_HP)

# plotting
#   TO DO:
#       - translate title!
line_color = [palette(:tab10)[1:3]; palette(:tab10)[7]; palette(:tab10)[4]]';
tick_years = Date.(2014:2021)
DateTick = Dates.format.(tick_years, "yyyy")

for flow in ["imports", "exports"]

    # count
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT, linecolor=line_color, xticks=false,
            group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="number of products", title="Flemish "* flow*": number of products")
    plot!(xticks=(tick_years, DateTick))
    vline!([Date(2016,6,23)], label="", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/formatted/" * "fig5_" * flow * "_product_count" * ".png")

    # HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT_HP, linecolor=line_color, xticks=false,
        group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="number of products", title="Flemish "* flow*": number of products (HP, λ=$λ)")
    plot!(xticks=(tick_years, DateTick))
    vline!([Date(2016,6,23)], label="", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/formatted/" * "fig5_" * flow * "_product_count_HP" * ".png")

end


df_export = subset(df, :FLOW => ByRow(x -> x == "exports"))[:,Not([:COUNT_HP])]
XLSX.writetable(dir_dropbox * "results/images/VLAIO/formatted/" * "data_figure4" * ".xlsx", df_export, overwrite=true)

df_export = subset(df, :FLOW => ByRow(x -> x == "imports"))[:,Not([:COUNT_HP])]
XLSX.writetable(dir_dropbox * "results/images/VLAIO/formatted/" * "data_figure5" * ".xlsx", df_export, overwrite=true)
