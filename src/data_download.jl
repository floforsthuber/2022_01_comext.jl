# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script downloading raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download raw data from Comext Bulk Download Facility
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

url = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F"

years = string.(2015:2020)
months = lpad.(1:12, 2, '0')

for i in years
    for j in months
        
        id = "full" * i * j * ".7z"

        if !isfile(dir_io * "raw/zipped/" * id)
            download(url * id, dir_io * "raw/zipped/" * id)
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
