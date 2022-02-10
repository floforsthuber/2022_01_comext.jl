
using LinearAlgebra, SparseArrays

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# HP filter (Hodrick–Prescott)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Wikipedia: https://en.wikipedia.org/wiki/Hodrick%E2%80%93Prescott_filter
#   The first term of the equation is the sum of the squared deviations, which penalizes the cyclical component. 
#   The second term is a multiple λ of the sum of the squares of the trend component's second differences. 
#   This second term penalizes variations in the growth rate of the trend component. The larger the value of λ, the higher is the penalty. 
#   Hodrick and Prescott suggest 1600 as a value for λ for quarterly data. 
#   Ravn and Uhlig (2002) state that λ should vary by the fourth power of the frequency observation ratio. 
#   Thus, λ should equal 6.25 (1600/4^4) for annual data and 129,600 (1600*3^4) for monthly data. 
#   In practice, λ = 100 for yearly data and λ = 14400 for monthly data are commonly used.


function HP(x::AbstractArray, λ::Int)
    n = length(x)
    m = 2
    @assert n > m
    I = Diagonal(ones(n))

    # use diagm instead of spdiagm otherwise error with grouped dataframe
    D = diagm(0 => fill(1, n-m),
        -1 => fill(-2, n-m),
        -2 => fill(1, n-m) )
    @inbounds D = D[1:n,1:n-m]

    return (I + λ * D * D') \ x
end



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 3
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "df_fig3" * ".csv"
df = CSV.read(path, DataFrame)

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
transform!(df, :PERIOD => ByRow(x -> string(x)[1:4]) => :YEAR)

# HP filter
cols_grouping = ["PARTNER_ISO", "FLOW", "YEAR"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :VALUE_SHARE => (x -> HP(x, 3)) => :VALUE_SHARE_HP)


flow = "imports"
# value share
p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :VALUE_SHARE_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Belgian "*flow)
vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end




# plotting
for flow in ["imports", "exports"]

    # value share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :VALUE_SHARE,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Belgian "*flow)
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig3/" * "fig3_" * flow * "_value_share" * ".png") # export image
    #savefig(p, dir_dropbox * "results/images/fig3/" * "fig3_" * flow * "_value_share" * ".png") # export image

    # quantity share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :QUANTITY_SHARE,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="kg", title="Belgian "*flow)
    vline!([Date(2016,6,23)], label="refer", color=:black, lw=2) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=2) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=2) # trans end
    savefig(p, dir_io * "clean/images/fig3/" * "fig3_" * flow * "_quantity_share" * ".png") # export image
    #savefig(p, dir_dropbox * "results/images/fig3/" * "fig3_" * flow * "_quantity_share" * ".png") # export image

end

