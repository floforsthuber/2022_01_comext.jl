# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script producing raw data for BE
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, Dates

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download raw data from Comext Bulk Download Facility
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

url = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F"

years = string.(2001:2021)
months = lpad.(1:12, 2, '0')

for i in years
    for j in months
        
        id = "full" * i * j * ".7z"

        if !isfile(dir_dropbox * "rawdata/comext/zipped/" * id)
            download(url * id, dir_dropbox * "rawdata/comext/zipped/" * id)
        else
            println(" ✓ The zipped file for $j/$i has already been downloaded.")
        end

    end
end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Unzip raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# trying to automate un-zipping, unfortunately failed => NEED TO MANUALLY UNZIPP to folder dir_io!
# run(`cmd /c set PATH=%PATH% ';' "C:\\Program Files\\7-Zip\\" echo %PATH% 7z`)
# run(`cmd /c cd "C:\\Users\\u0148308\\data\\raw\\"`)
# run(`cmd /c 7z e a.7z`)

# manually

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Initial cleaning, subset and export data for BE as DECLARANT
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# EU27
ctrys_EU27 = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU",
             "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

for i in years
    for j in months

        path = dir_dropbox * "rawdata/comext/BE/" * "full" * i * j * "_BE" * ".csv"

        if isfile(path)
            println(" ✓ Data for BE in $j/$i already exists! \n")
        else
            # import and clean data
            df = initial_cleaning(i, j)

            # subset to include only BE as DECLARANT
            subset!(df, :DECLARANT_ISO => ByRow(x -> x == "BE"))

            # create EU/WORLD as PARTNER
            df = append_WORLD(df)
            df = append_EU(df, ctrys_EU27)

            # export to Dropbox
            CSV.write(path, df)

            println(" ✓ Data for BE in $j/$i has been successfully exported to Dropbox. \n")
        end

    end
end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
