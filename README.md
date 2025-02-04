# Purpose

To download the time series data from Aquarius and organise the data in `DataFrame`s.

# Requirements

- Install [PowerShell](https://github.com/PowerShell/PowerShell) and [its vscode extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode.PowerShell)
- Download the [ReportRunner](https://github.com/AquaticInformatics/getting-started/releases/ReportRunner) executable file and save it into folder `_tools`
- To process the downloaded CSV files (in folder `out/csv/`). Note: You don't have to download/install DuckDB/Python/R at the same time. Just pick one as each of them will achieve the same goal (described in the scripts).

    - To run [DuckDB](https://duckdb.org) to process the CSV files downloaded from Aquarius:
      ```powershell
      # Install DuckDB CLI on Windows
      > winget install DuckDB.cli
      # Or update the existing DuckDB CLI on Windows
      > winget upgrade DuckDB.cli
      ```
    - To run the [Python](https://www.microsoft.com/store/productId/9NRWMJP3717K?ocid=pdpshare) scripts, run the following to install the needed modules:
      ```powershell
      # Create a virtual environment after unzip or clone
      > python -m venv venv
      # Activate the environment
      > venv\Scripts\activate.bat
      # Install the required modules
      > pip install -r requirements.txt
      ```
    - To run the [R](https://cran.r-project.org/) scripts, the following packages are needed:
      - [httr](https://cran.r-project.org/web/packages/httr/index.html)
      - [data.table](https://cran.r-project.org/web/packages/data.table/index.html)
      - [arrow](https://cran.r-project.org/web/packages/arrow/index.html)
      - [duckdb](https://cran.r-project.org/web/packages/duckdb/index.html)
      
      ```r
      # Install the above R packages
      > install.packages(c("httr", "data.table", "arrow"))
      ```
