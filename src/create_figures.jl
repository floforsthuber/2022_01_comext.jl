# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)

# Notes:
#   - figure 1: evolution of total imports/exports from BE to UK vis-a-vis FR, NL, DE and EU
#   - figure 2: evolution of imports/exports from BE to UK at 2digit level
#   - figure 3: evolution of share of imports/exports from BE to UK of total imports/exports from BE
#   - all figures in values and unit values (unit prices):
#       + compute a price index as using a weighted sum
#   - include vertical lines for different Brexit events

# -------------------------------------------------------------------------------------------------------------------------------------------------------------



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "df_fig1" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# manipulation
# Notes:
#   - there are exceptionally big observations in some months (needs further investigation), remove for now
transform!(df, :PRICE_INDEX => ByRow(x -> ifelse(x > 20_000, missing, x)), renamecols=false)

# plotting
# Notes:
#   - unit prices are extremely volatile, 3 MMA would be useful

for flow in ["imports", "exports"]

    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :VALUE_IN_EUROS,
            group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros", title="Belgian "*flow)
    savefig(p, dir_io * "clean/images/" * "fig1_" * flow * "_values" * ".png") # export image

    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX,
        group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="unit prices", title="Belgian "*flow)
    savefig(p, dir_io * "clean/images/" * "fig1_" * flow * "_prices" * ".png") # export image

end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "df_fig2" * ".csv"
df = CSV.read(path, DataFrame)
