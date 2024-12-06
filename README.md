# MongoCDWImport

A small app to query our MongoDB and import data into CDW for reporting.

## Description

This application connects to a MongoDB database, performs queries to retrieve and process data, and then imports the processed data into a CDW (Clinical Data Warehouse) for reporting purposes. The application uses `moment` for date manipulation, `fast-csv` for CSV file generation, and `mongodb` for database operations.

## Installation

1. Clone the repository:
    ```sh
    git clone <repository-url>
    cd MongoCDWImport
    ```

2. Install the dependencies:
    ```sh
    npm install
    ```

3. Configure the application:
    - Copy `config.js.sample` to `config.js` and fill in the required configuration details.
    - Copy `db.js.sample` to `db.js` and fill in the MongoDB URL.

4. Set up an SSH tunnel to connect to the database:
    ```sh
    ssh -L <local-port>:<db-host>:<db-port> <ssh-user>@<ssh-host>
    ```
    Replace `<local-port>`, `<db-host>`, `<db-port>`, `<ssh-user>`, and `<ssh-host>` with the appropriate values.

5. Run the application:
    ```sh
    node index.js
    ```

## Usage

The application processes dates for a specified year and imports the data into the CDW. Modify the `year` variable in [index.js](http://_vscodecontentref_/1) to specify the year you want to process. Note that the application uses Fileman dates for date manipulation. For more information on Fileman dates, you can refer to [this guide](https://www.vistapedia.com/index.php/Date_formats).

## search2Excel.js

The `search2Excel.js` file performs similar operations as the main application but instead of importing the data into the CDW, it outputs the data to an Excel file. This can be useful for generating reports or analyzing data in a spreadsheet format.

To run `search2Excel.js`, use the following command:
```sh
node search2Excel.js
